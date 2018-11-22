# kvproto
Protocol buffer files for TiKV

# Usage

+ Write your own protocol file in proto folder.
+ If you need to update raft-rs, please download the proto file
    respectively and overwrite the one in include folder.
+ Run `make` to generate go and rust code. 
    We generate all go codes in pkg folder and rust in src folder.
+ Update the dependent projects.

