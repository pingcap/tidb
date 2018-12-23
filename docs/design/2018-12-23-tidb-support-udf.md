##Proposal: Tidb supporting UDF with lua
* Author(s):  (@haoxiang47 @TennyZhuang @jiangplus) 
* Last updated:
* Discussion at: [tidb supporting udf with lua](https://docs.google.com/document/d/1jThyalBWLUgKrcy5NoghciEBnAlVvny_a1iLdUQa_3A/edit#)

##Abstract

We know that now tidb do not support UDF in sql, so we want to support UDF with Lua in tidb, users can write lua script to be the UDF and call it later.
##Background
This idea just from the pingcap hackathon 2018, our team(@haoxiang47 @TennyZhuang @jiangplus) implemented a Lua based UDF. Now we want to continue this idea.
##Proposal
In this proposal we want to discuess three parts of this idea.

 * Is lua best choice to implement the UDF script?
 * The steps about how we want to implement this idea.
 * The expand of this idea


1. First of all, we should add CREATE FUNCTION syntax to tidb, which is not currently supported by the parser, and all function names are hard coded in the source code. we will need to update https://github.com/pingcap/parser to add the new syntax first.
We should be able to embed lua script to the sql syntax, also making sure the syntax is escapable.
2. We will add a UDF serving api to PD, to register and load UDF which user creates.
3. Whenever new UDFs are created, it should be pushed down to tidb and tikv nodes.
4. Make the UDF work better with the tidb query planner
5. We should implement a better coprocessor to call lua in tikv. In hackathon we just use hlua to run a lua vm, but in production env, we should call lua in a new process, and set cpu and memory control, to avoiding performance skews with tikv server.
   
All above are our steps to implement this idea, for 3), one way is to push down all the UDF to tikv, another way is to run lua script in tidb directly, maybe we more inclined the latter, we think tidb is a compute layer, is lighter and safety, if the lua process fails, it will not affect the storage.
##Rationale
After TitanDB storage engine is used on TiKV, we can save large blob in our table, which will increase large cost in bandwidth during selection. And in some cases, we only need a little information in the blob, which cannot be represented by SQL built-in function because lack of turing-completeness, e.g., extract some meta in jpg blob, sample some frame in video blob, or in deep learning case, run some feature extraction function on origin blob.

Note: Use cases above are dependent on TitanDB engine and projection operator pushdown, which have not been implemented in TiDB latest release.
##Compatibility
This implement will have a huge change in tidb/tikv/tipb/pd, in tikv/tipb/pd change will keep compatibility easily, in tidb’s change, we should keep compatibility in builtin function’s push down logic.
##Implementation
TBD

1. Add function management API in PD, we should save function body (bytecode in lua for JIT) in PD for global access in the whole cluster.
2. Modify parser to support `CREATE FUNCTION` syntax and call UDF syntax (use CallLua explicitly or just like built-in function?). User should declare the arguments type and DETERMINISTIC of the funciton.
3. Implement function loading from PD and calling with lua VM in both TiDB and TiKV, for determined function, we can push down it to TiKV, otherwise we can fallback to calculate it in TiDB.


##Open issues (if applicable)
https://github.com/pingcap/tidb/issues/8557
