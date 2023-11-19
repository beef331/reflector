# Reflector

This is a very simple local file cloner for making duplications from one drive or folder to another.
It is 'constructive only' meaning that deletions do not propogate only mutations and creations.
Deleting a file that is mirrored will not delete it on the mirror locations

Presently configuration is done by making a `~/.config/reflector/config` or `$XDG_CONFIG_PATH/reflector/config` file each line of the file is a mirror source and location.
An example config is as simple as:
```
/home/jason/->/mnt/otherdrive/home
```

You then can start the daemon anyway you want.
I personally use `systemctl` to start a service.

## Why does this exist when there are others that are more capable?

I am clearly a numpty that felt like throwing together a simple inotify daemon.


