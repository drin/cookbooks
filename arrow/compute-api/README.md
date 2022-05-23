# Overview

This recipe is for calling the `Index` arrow compute function. I personally prefer `meson` to `cmake`, so I have included a `meson.build` file here.


# Build Instructions

I wrote this using `meson 0.62.0`:
```bash
>> meson --version
0.62.0
```

I compile the code using the following commands:
```bash
>> meson build
>> meson compile -C build
```

Meson uses ninja for compilation, so some options (such as `-C`) seem to forward to ninja.

Here's what my session looks like, using `...` to make it more concise:
```
>> meson build
The Meson build system
Version: 0.62.0
...
Found pkg-config: /usr/bin/pkg-config (1.8.0)
Run-time dependency arrow-dataset found: YES 7.0.0
Build targets in project: 1

>> meson compile -C build
ninja: Entering directory `/.../cookbooks/arrow/compute-api/build'
[1/3] Compiling C++ object index-recipe.p/index.cpp.o
...
[3/3] Linking target index-recipe
```

# Execution

This line in `meson.build` defines the binary that will be created:
```
exe_recipe = executable('index-recipe'
    ...
)
```

Specifically, this creates the binary `index-recipe` in the `build` directory (or whatever name you use in `meson <directory name>`).

So, running this recipe is extremely easy:
```bash
>> ./build/index-recipe
Index of value [val2]: 2
```
