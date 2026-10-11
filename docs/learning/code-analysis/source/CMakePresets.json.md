# CMakePresets.json

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；123行。

[对应分析](../06-build-and-entry.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~json
{
    "version": 4,
    "cmakeMinimumRequired": {
        "major": 3,
        "minor": 23,
        "patch": 0
    },
    "configurePresets": [
        {
            "name": "gcc15-base",
            "hidden": true,
            "generator": "Unix Makefiles",
            "condition": {
                "type": "equals",
                "lhs": "${hostSystemName}",
                "rhs": "Linux"
            },
            "cacheVariables": {
                "CMAKE_C_COMPILER": "/usr/bin/gcc-15",
                "CMAKE_CXX_COMPILER": "/usr/bin/g++-15",
                "CMAKE_BUILD_TYPE": "Debug",
                "CMAKE_EXPORT_COMPILE_COMMANDS": true,
                "ARENA_ENABLE_UBSAN": true
            }
        },
        {
            "name": "gcc15",
            "displayName": "GCC 15 Debug",
            "inherits": "gcc15-base",
            "binaryDir": "${sourceDir}/build"
        },
        {
            "name": "gcc15-asan",
            "displayName": "GCC 15 ASan / UBSan",
            "inherits": "gcc15-base",
            "binaryDir": "${sourceDir}/build-asan",
            "cacheVariables": {
                "CMAKE_CXX_FLAGS": "-fsanitize=address,undefined -fno-omit-frame-pointer",
                "CMAKE_EXE_LINKER_FLAGS": "-fsanitize=address,undefined"
            }
        },
        {
            "name": "gcc15-tsan",
            "displayName": "GCC 15 TSan",
            "inherits": "gcc15-base",
            "binaryDir": "${sourceDir}/build-tsan",
            "cacheVariables": {
                "CMAKE_CXX_FLAGS": "-fsanitize=thread -fno-omit-frame-pointer -g",
                "CMAKE_EXE_LINKER_FLAGS": "-fsanitize=thread -no-pie",
                "ARENA_ENABLE_UBSAN": false
            }
        }
    ],
    "buildPresets": [
        {
            "name": "gcc15",
            "configurePreset": "gcc15",
            "jobs": 4
        },
        {
            "name": "gcc15-asan",
            "configurePreset": "gcc15-asan",
            "jobs": 4
        },
        {
            "name": "gcc15-tsan",
            "configurePreset": "gcc15-tsan",
            "jobs": 4
        }
    ],
    "testPresets": [
        {
            "name": "gcc15",
            "configurePreset": "gcc15",
            "output": {
                "outputOnFailure": true
            },
            "filter": {
                "exclude": {
                    "name": "Performance"
                }
            },
            "execution": {
                "jobs": 4,
                "noTestsAction": "error"
            }
        },
        {
            "name": "gcc15-asan",
            "configurePreset": "gcc15-asan",
            "output": {
                "outputOnFailure": true
            },
            "filter": {
                "exclude": {
                    "name": "Performance"
                }
            },
            "execution": {
                "jobs": 4,
                "noTestsAction": "error"
            }
        },
        {
            "name": "gcc15-tsan",
            "configurePreset": "gcc15-tsan",
            "description": "Reactor/TCP ownership and four-worker network acceptance; excludes GTest death and FD-exhaustion tests.",
            "output": {
                "outputOnFailure": true
            },
            "filter": {
                "include": {
                    "name": "^(EventLoopThreadPoolTest|EventLoopThreadTest|EventLoopTest|TcpServerWorkerTest|TcpServerTest|TcpServerOwnershipTest|TcpConnectionTest|ConnectionCallbackTest)\\.|^TcpTermination/ConnectionOwnershipTest\\.|^TcpServerWorkers_"
                }
            },
            "execution": {
                "jobs": 1,
                "timeout": 60,
                "noTestsAction": "error"
            }
        }
    ]
}
~~~
