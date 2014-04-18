# Bitcask - a key/value datastore

It is very simply now. Keep going ... ...

## Set

"Set" means "store this data". 

## Add

"Add" means "store this data, but only if the server *doesn't* already
  hold data for this key". 

## Delete  

Delete data by key, in fact, just set a empty value for that key.

## Get

"Get" data by key.

## Merge

Merge the files.

## TODO
bitcask.Merge