import std/[
  paths,
  files,
  dirs,
  appdirs,
  tables,
  strscans,
  oserrors,
  strformat,
  asyncfile,
  asyncdispatch,
  asyncstreams,
  sequtils,
  hashes,
  sets,
  times
]
import std/inotify except InotifyEvent
import os except getConfigDir, getCacheDir
import std/logging
import pkg/slicerator
# import pkg/syslog # Does using syslog make any sense‽

type Unimplemented = object of CatchableError

addHandler newConsoleLogger()
addHandler newFileLogger(string (getCacheDir() / Path"reflector"))

proc error(s: string) =
  try:
    logging.error(s)
  except Exception as e:
    echo "Fail to log ERROR: ", e.msg

proc info(s: string) =
  try:
    logging.info(s)
  except Exception as e:
    echo "Fail to log INFO: ", e.msg

proc fatal(s: string) =
  try:
    logging.fatal(s)
    try:
      discard execShellCmd(fmt"notify-send -c critical 'Reflector: {s}'")
    except: discard
  except Exception as e:
    echo "Failed to log FATAL: ", e.msg


template unimplemented(s: string): untyped =
  raise (ref Unimplemented)(msg: "Unimplemented: " & s)


const 
  pathScan = "$+->$+"
  pathMax = 4096

let cfgPaths = 
  [
    getConfigDir() / Path"reflector" / Path"config"
  ]


type
  InotifyFlag = enum
    Access = 1
    Modify
    Attrib
    CloseWrite
    CloseNoWrite
    Open
    MovedFrom
    MovedTo
    Create
    Delete
    DeleteSelf
    MoveSelf
    
    Unmount = 14
    QueueOverflow
    Ignored
    
    OnlyDir = 25
    DontFollow
    ExclUnlink
    
    OnlyCreateWatch = 29 
    AddToWatch
  
    IsDir
    OneShot

  InotifyFlags = set[InotifyFlag]

  InotifyEvent {.pure, final, importc: "struct inotify_event",
                 header: "<sys/inotify.h>".} = object
    wd* {.importc: "wd".}: FileHandle ## Watch descriptor.
    mask* {.importc: "mask".}: InotifyFlags ## Watch mask.
    cookie* {.importc: "cookie".}: uint32 ## Cookie to synchronize two events.
    len* {.importc: "len".}: uint32 ## Length (including NULs) of name.
    name* {.importc: "name".}: UncheckedArray[char] ## Name.

converter toCint*(i: InotifyFlags): uint32 = copyMem(result.addr, i.addr, sizeof cint)

when sizeof(InotifyFlags) != sizeof(uint32):
  {.error: fmt"Mismatch between flags({sizeof InotifyFlags}) and uint32({sizeof(uint32)})".}

proc getName*(evt: ptr InotifyEvent): string = $cast[cstring](evt.name.addr)

const 
  Close = {CloseWrite, CloseNoWrite}
  Moved = {MovedFrom, MovedTo}

proc hash(p: Path): Hash = hash(string p)
proc `$`(p: Path): string = string p


proc createWatcherRecursively(fd: cint, path: Path, watchers: var Table[cint, Path], mirrors: var Table[Path, HashSet[Path]], parent: Path) =
  let watchdog = fd.inotify_add_watch(cstring path, {Create, Attrib, Modify} + Moved)
  if watchdog < 0:
    try:
      error(fmt"Failed to watch '{string path}': {osErrorMsg(osLastError())}.")
    except:
      raise

  if parent != Path"":
    let 
      parentPaths = mirrors[parent]
      tail = path.splitPath.tail
    var destPaths = initHashSet[Path](parentPaths.len)
    for parDir in parentPaths:
      destPaths.incl parDir / tail
    mirrors[path] = destPaths

  watchers[watchdog] = path
  if dirExists path:
    for dir in path.walkDir():
      if dir.kind == pcDir:
        createWatcherRecursively(fd, dir.path, watchers, mirrors, path)

proc clone(src, dest: Path, srcModifcation: Time, creating: bool = false): Future[void] {.async.} =
  if creating:
    info fmt"Created '{dest}' due to '{src}'"
  else:
    info fmt"Copying from '{src}' to '{dest}'."

  let 
    srcFile = openAsync(string src, fmRead)
    destFile = openAsync(string dest, fmWrite)
  defer:
    srcFile.close()
    destFile.close()

  let fs = newFutureStream[string]()
  await srcFile.readToStream(fs)
  await destFile.writeFromStream(fs)
  setLastModificationTime(string src, srcModifcation)


proc clone(src, dest: Path, futures: var seq[Future[void]]) = # So many TOCTOU race conditions
  if dirExists(src) and not dirExists(dest): # No Future required here
    info fmt"Copying from '{src}' to '{dest}'."
    copyDir(string src, string dest)
  elif dirExists(src) and dirExists(dest):
    for dir in walkDir(src, relative = true):
      clone(src / dir.path, dest / dir.path, futures)
  elif fileExists(src):
    let srcModifcation = getLastModificationTime(string src)
    if not fileExists(dest) or srcModifcation > getLastModificationTime(string dest):
      futures.add clone(src, dest, srcModifcation)
  elif symLinkExists(string src):
    info fmt"Skipping symlink '{src}'"
  else:
    raise (ref ValueError)(msg: fmt"Unable to handle the configuration of: '{src}' -> '{dest}'.")
  if futures.len == 100:
    waitfor all futures
    futures.setLen 0 



proc loadConfig(
  watcher: cint;
  configWatchers, watchers: var Table[cint, Path];
  mirrors: var Table[Path, HashSet[Path]];
  futures: var seq[Future[void]]
) =
  let origMirrors = mirrors.keys.toSeq.toHashSet()
  mirrors = initTable[Path, HashSet[Path]](mirrors.len)
  for path in cfgPaths:
    try:
      var src, dest = ""
      for line in path.string.lines:
        if line.scanf(pathScan, src, dest):
          let (src, dest) = (Path src, Path dest)
          try:
            clone(src, dest, futures)
          except Exception as e:
            echo e.msg
          if mirrors.hasKeyOrPut(src, [dest].toHashset):
            mirrors[src].incl dest
      block addIfNew:
        for value in configWatchers.values:
          if value == path:
            break addIfNew

        let fd = watcher.inotifyAddWatch(cstring path, {Modify, Create})
        if fd >= 0:
          configWatchers[fd] = path

    except CatchableError as e:
      error fmt"Failed to load config from {path}: '{e.msg}'"
      return

  for mirror in origMirrors: # Handle mirrors being removed
    if mirror notin origMirrors:
      for key, val in watchers.pairs:
        if val == mirror:
          watchers.del key
          break


proc main(): Future[void] {.async.} =
  var 
    mirrors: Table[Path, HashSet[Path]]
    watchesToMirror: Table[cint, Path]
    configWatchers: Table[cint, Path]
    futures: seq[Future[void]]

  let watcherFd = inotify_init1(0)
  if watcherFd < 0:
    fatal "Failed to intialize inotify: " & osErrorMsg(osLastError())
    return
  
  loadConfig(watcherFd, configWatchers, watchesToMirror, mirrors, futures)

  if futures.len > 0:
    try:
      waitfor all futures
    except Exception as e:
      error e.msg
      return
      

  if mirrors.len == 0:
    info("No files or paths to mirror")


  var watcherFile = 
    try:
      newAsyncFile(AsyncFd watcherFd)
    except CatchableError as e:
      fatal "Could not open inotify file: " & e.msg
      return

  let startMirrors = mirrors.keys.toSeq
  for mirror in startMirrors:
    try:
      watcherFd.createWatcherRecursively(mirror, watchesToMirror, mirrors, Path"")
    except Exception as e:
      fatal "Failed to recursively add watchers for '" & string(mirror) & "': " & e.msg
      return

  var 
    buffer = newString(sizeof(InotifyEvent) + pathMax + 1)
    movedFromBuffer: Table[uint32, (Path, Path)] # (Parent, Name)

  info("Done syncing directories")
  while true:
    try:
      let len = await watcherfile.readBuffer(buffer[0].addr, buffer.len)
      var pos = 0
      while pos < len:
        var event = cast[ptr InotifyEvent](buffer[pos].addr)
        if event.wd in configWatchers:
          info "Reloading config"
          loadConfig(watcherFd, configWatchers, watchesToMirror, mirrors, futures)

        elif event.wd in watchesToMirror:
          let src = watchesToMirror[event.wd]

          if event.mask in [{MovedFrom}, {MovedFrom, IsDir}]:
            if event.cookie != 0:
              movedFromBuffer[event.cookie] = (src, Path event.getName())
          elif event.mask in [{MovedTo, IsDir}, {MovedTo}]:
            if event.cookie != 0:
              let 
                moveFrom = mirrors[movedFromBuffer[event.cookie][0]]
                moveTo = mirrors[src]
                srcFileName = movedFromBuffer[event.cookie][1]
              assert moveFrom.len == moveTo.len
              for (frm, to) in slicerator.zip(moveFrom.items, moveTo.items):
                let
                  src = frm / srcFileName
                  dest = to / Path event.getName()
                info fmt"Moving '{src}' to '{dest}'"
                moveFile(src, dest)
              movedFromBuffer.del event.cookie
            else:
              unimplemented fmt"Move to without from: {src}"
          elif event.mask in [{Create}, {Create, IsDir}, {Modify}]:
            let 
              fileName = Path event.getName()
              srcPath = watchesToMirror[event.wd]
            for destPath in mirrors[watchesToMirror[event.wd]]:
              let
                src = srcPath / fileName
                dest = destPath / fileName
              await clone(src, dest, getLastModificationTime(string src), true)

          elif event.mask in [{Attrib}]:
            info fmt"Skipping {event.mask} unimplemented but not errory"
          else:
            unimplemented fmt"Failed operation {event.mask}"


        pos += sizeof(InotifyEvent) + int event.len
      
    except Exception as e:
      fatal "Failed to read: " & e.msg
      break

try:
  waitfor main()
except Exception as e:
  error e.msg
