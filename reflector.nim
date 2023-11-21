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
import reflector/inotifyevents
# import pkg/syslog # Does using syslog make any sense‽

type Reflector* = ref object
  handlers*: Table[InotifyFlags, proc(_: Reflector, _: ptr InotifyEvent, src: Path): Future[void] {.async.}]
  mirrors*: Table[Path, HashSet[Path]]
  watchers*: Table[cint, Path]
  configWatchers*: Table[cint, Path]
  movedFromBuffer*: Table[uint32, (Path, Path)] # (Parent, Name)
  futures*: seq[Future[void]]
  watcher*: cint


type Unimplemented = object of CatchableError


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


proc hash(p: Path): Hash = hash(string p)
proc `$`(p: Path): string = string p

proc createWatcherRecursively*(reflectObj: Reflector, path: Path, parent: Path) =
  let watchdog = reflectObj.watcher.inotify_add_watch(cstring path, {Create, Attrib, Modify} + Moved)
  if watchdog < 0:
    try:
      error(fmt"Failed to watch '{string path}': {osErrorMsg(osLastError())}.")
    except:
      raise

  if parent != Path"":
    let 
      parentPaths = reflectObj.mirrors[parent]
      tail = path.splitPath.tail
    var destPaths = initHashSet[Path](parentPaths.len)
    for parDir in parentPaths:
      destPaths.incl parDir / tail
    reflectObj.mirrors[path] = destPaths

  reflectObj.watchers[watchdog] = path
  if dirExists path:
    for dir in path.walkDir():
      if dir.kind == pcDir:
        createWatcherRecursively(reflectObj, dir.path, path)

proc clone*(src, dest: Path, srcModifcation: Time, creating: bool = false): Future[void] {.async.} =
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


proc clone*(src, dest: Path, futures: var seq[Future[void]]) = # So many TOCTOU race conditions
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


proc loadConfig*(reflectObj: Reflector) =
  let origMirrors = reflectObj.mirrors.keys.toSeq.toHashSet()
  reflectObj.mirrors = initTable[Path, HashSet[Path]](reflectObj.mirrors.len)
  for path in cfgPaths:
    try:
      var src, dest = ""
      for line in path.string.lines:
        if line.scanf(pathScan, src, dest):
          let (src, dest) = (Path src, Path dest)
          try:
            clone(src, dest, reflectObj.futures)
          except Exception as e:
            echo e.msg
          if reflectObj.mirrors.hasKeyOrPut(src, [dest].toHashset):
            reflectObj.mirrors[src].incl dest
      block addIfNew:
        for value in reflectObj.configWatchers.values:
          if value == path:
            break addIfNew

        let fd = reflectObj.watcher.inotifyAddWatch(cstring path, {Modify, Create})
        if fd >= 0:
          reflectObj.configWatchers[fd] = path

    except CatchableError as e:
      error fmt"Failed to load config from {path}: '{e.msg}'"
      return

  for mirror in origMirrors: # Handle mirrors being removed
    if mirror notin origMirrors:
      for key, val in reflectObj.watchers.pairs:
        if val == mirror:
          reflectObj.watchers.del key
          break

proc route*(reflectorObj: Reflector, keys: varargs[InotifyFlags],  p: proc(_: Reflector, _: ptr InotifyEvent, _: Path): Future[void] {.async.}) =
  for key in keys:
    reflectorObj.handlers[key] = p

proc dispatch*(reflectorObj: Reflector, event: ptr InotifyEvent) {.async.} =
  if event.wd in reflectorObj.configWatchers:
    info "Reloading config"
    loadConfig(reflectorObj)
    if reflectorObj.futures.len > 0:
      await all reflectorObj.futures
      reflectorObj.futures.setLen(0)
  elif event.wd in reflectorObj.watchers and event.mask in reflectorObj.handlers:
    await reflectorObj.handlers[event.mask](reflectorObj, event, reflectorObj.watchers[event.wd])
  else:
    unimplemented fmt" no handler for {event.mask} operating on '{event.wd}'"

when isMainModule:
  addHandler newConsoleLogger()
  addHandler newFileLogger(string (getCacheDir() / Path"reflector"))

  proc main(): Future[void] {.async.} =
    var reflectObj = Reflector(watcher: inotify_init1(0))

    reflectObj.route {MovedFrom}, {MovedFrom, IsDir}, proc(refl: Reflector, event: ptr InotifyEvent, src: Path) {.async.} = 
      if event.cookie != 0:
        refl.movedFromBuffer[event.cookie] = (src, Path event.getName())

    reflectObj.route {MovedTo}, {MovedTo, IsDir}, proc (refl: Reflector, event: ptr InotifyEvent, src: Path) {.async.} =
      if event.cookie != 0:
        let 
          moveFrom = refl.mirrors[refl.movedFromBuffer[event.cookie][0]]
          moveTo = refl.mirrors[src]
          srcFileName = refl.movedFromBuffer[event.cookie][1]
        assert moveFrom.len == moveTo.len
        for (frm, to) in slicerator.zip(moveFrom.items, moveTo.items):
          let
            src = frm / srcFileName
            dest = to / Path event.getName()
          info fmt"Moving '{src}' to '{dest}'"
          moveFile(src, dest)
        refl.movedFromBuffer.del event.cookie
      else:
        unimplemented fmt"Move to without from: {src}"

    reflectObj.route {Create}, {Create, IsDir}, {Modify}, proc(refl: Reflector, event: ptr InotifyEvent, src: Path) {.async.} =
      let 
        fileName = Path event.getName()
      for destPath in refl.mirrors[refl.watchers[event.wd]]:
        let
          src = src / fileName
          dest = destPath / fileName
        if IsDir in event.mask:
          clone(src, dest, refl.futures)
          if refl.futures.len > 0:
            await all refl.futures
            refl.futures.setLen(0)
        else:
          await clone(src, dest, getLastModificationTime(string src), true)

    reflectObj.route {Attrib}, {Attrib, IsDir}, proc(refl: Reflector, event: ptr InotifyEvent, _: Path){.async.} =
      info fmt"Skipping {event.mask} unimplemented but not errory"

    if reflectObj.watcher < 0:
      fatal "Failed to intialize inotify: " & osErrorMsg(osLastError())
      return
    
    loadConfig(reflectObj)

    if reflectObj.futures.len > 0:
      try:
        waitfor all reflectObj.futures
      except Exception as e:
        error e.msg
        return
        

    if reflectObj.mirrors.len == 0:
      info("No files or paths to mirror")


    var watcherFile = 
      try:
        newAsyncFile(AsyncFd reflectObj.watcher)
      except CatchableError as e:
        fatal "Could not open inotify file: " & e.msg
        return

    let startMirrors = reflectObj.mirrors.keys.toSeq
    for mirror in startMirrors:
      try:
        reflectObj.createWatcherRecursively(mirror, Path"")
      except Exception as e:
        fatal "Failed to recursively add watchers for '" & string(mirror) & "': " & e.msg
        return

    var 
      buffer = newString(sizeof(InotifyEvent) + pathMax + 1)

    info("Done syncing directories")
    while true:
      try:
        let len = await watcherfile.readBuffer(buffer[0].addr, buffer.len)
        var pos = 0
        while pos < len:
          var event = cast[ptr InotifyEvent](buffer[pos].addr)
          await reflectObj.dispatch(event)

          pos += sizeof(InotifyEvent) + int event.len
        
      except Exception as e:
        fatal "Failed to read: " & e.msg
        break

  try:
    waitfor main()
  except Exception as e:
    error e.msg
