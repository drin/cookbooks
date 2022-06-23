# Overview

This recipe is for calling `HashBatch`, a low-level function for calculating hashes given an `ExecBatch`.

I personally prefer `meson` to `cmake`, so I have included a `meson.build` file here.


# Build Instructions

I wrote this using `meson 0.62.2`:
```bash
>> meson --version
0.62.2
```

I compile the code using the following commands:
```bash
>> meson build
>> meson compile -C build
```

Meson uses ninja for compilation, so some options (such as `-C`) seem to forward to ninja.

Here's what my session looks like, using `...` to make it more concise:
```bash
>> meson build
The Meson build system
Version: 0.62.2
...
Found pkg-config: /usr/bin/pkg-config (1.8.0)
Run-time dependency arrow-dataset found: YES 9.0.0-SNAPSHOT
Build targets in project: 1

>> meson compile -C build
ninja: Entering directory `/.../cookbooks/arrow/hash-functions/build'
[1/3] Compiling C++ object hash-recipe.p/hash.cpp.o
...
[3/3] Linking target hash-recipe
```

# Execution

This line in `meson.build` defines the binary that will be created:
```python
exe_recipe = executable('hash-recipe'
    ...
)
```

Specifically, this creates the binary `hash-recipe` in the `build` directory (or whatever name you use in `meson <directory name>`).

So, running this recipe is extremely easy:
```bash
>> ./build/hash-recipe
/.../arrow/compute/exec/key_hash.cc:509: Creating LightContext for hashing
/.../arrow/compute/exec/key_hash.cc:514: Calling HashMultiColumn
/.../arrow/compute/exec/key_hash.cc:381: Hashing multi-column...
/.../arrow/compute/exec/key_hash.cc:384: Constructing TempVectorHolder (temp buf)
/.../arrow/compute/exec/util.h:120: 4176 (top_) <= 11664 (buffer_size_)
/.../arrow/compute/exec/key_hash.cc:388: Constructing TempVectorHolder (null indices buf)
/.../arrow/compute/exec/util.h:120: 6304 (top_) <= 11664 (buffer_size_)
/.../arrow/compute/exec/key_hash.cc:393: Constructing TempVectorHolder (null temp buf)
/.../arrow/compute/exec/util.h:120: 10480 (top_) <= 11664 (buffer_size_)
/.../arrow/compute/exec/key_hash.cc:401: Hashing batch offset: [0, 5)
Result Hashes:
        1388365485
        4015522576
        1233526310
        834694783
        1074713562
Hash status: OK
```
