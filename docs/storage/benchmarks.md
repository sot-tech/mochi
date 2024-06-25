# Hardware

* CPU: Intel i5-12500H
* RAM: 16GiB (2x8 Samsung M471A1K43EB1-CWE)
* Storage: NVME SSD Samsung 980PRO
* OS: Ubuntu 22.04

# Benchmarks
# Memory

```
goos: linux
goarch: amd64
pkg: github.com/sot-tech/mochi/storage/memory
cpu: 12th Gen Intel(R) Core(TM) i5-12500H
BenchmarkStorage/BenchmarkNop-16                1000000000               0.1822 ns/op          0 B/op          0 allocs/op
BenchmarkStorage/BenchmarkPut-16                 7071364               173.1 ns/op            80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkPut1k-16               5342302               280.4 ns/op            80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash-16              17775769                65.81 ns/op           80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash1k-16            15953836                68.41 ns/op           80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkPutDelete-16                   4975660               237.0 ns/op           160 B/op          4 allocs/op
BenchmarkStorage/BenchmarkPutDelete1k-16                 4842673               240.3 ns/op           160 B/op          4 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash-16         4597555               248.2 ns/op           160 B/op          4 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash1k-16       4776769               265.1 ns/op           160 B/op          4 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist-16             19164670                60.77 ns/op           96 B/op          3 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1k-16           29773471                42.81 ns/op           96 B/op          3 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash-16           38686660                32.11 ns/op           96 B/op          3 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash1k-16         36604658                32.88 ns/op           96 B/op          3 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete-16                       2982174               419.8 ns/op           240 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1k-16                     2881537               407.3 ns/op           240 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash-16             2654642               443.4 ns/op           240 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash1k-16           2630800               462.8 ns/op           240 B/op          6 allocs/op
BenchmarkStorage/BenchmarkGradNonexist-16                        6837140               203.3 ns/op            80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1k-16                      5347960               267.2 ns/op            80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash-16             15773694                75.18 ns/op           80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash1k-16           16675423                73.16 ns/op           80 B/op          2 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher-16                     1368090               823.9 ns/op          4496 B/op          3 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher1kInfoHash-16           1401063               823.8 ns/op          4496 B/op          3 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder-16                      4446224               273.7 ns/op          1424 B/op          2 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder1kInfoHash-16            4279449               280.8 ns/op          1424 B/op          2 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm-16                        16012303                67.37 ns/op           16 B/op          1 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm1kInfoHash-16              82122622                14.58 ns/op           16 B/op          1 allocs/op
PASS
ok      github.com/sot-tech/mochi/storage/memory        41.848s
```

# Redis

Version: 6.0.16

Configuration: OOTB

```
goos: linux
goarch: amd64
pkg: github.com/sot-tech/mochi/storage/redis
cpu: 12th Gen Intel(R) Core(TM) i5-12500H
BenchmarkStorage/BenchmarkNop-16                1000000000               0.1611 ns/op          0 B/op          0 allocs/op
BenchmarkStorage/BenchmarkPut-16                  180381              6148 ns/op            1257 B/op         37 allocs/op
BenchmarkStorage/BenchmarkPut1k-16                203150              6314 ns/op            1273 B/op         37 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash-16                196033              6187 ns/op            1249 B/op         37 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash1k-16              202513              6176 ns/op            1265 B/op         37 allocs/op
BenchmarkStorage/BenchmarkPutDelete-16                     26925             40429 ns/op            1736 B/op         56 allocs/op
BenchmarkStorage/BenchmarkPutDelete1k-16                   27751             39310 ns/op            1768 B/op         56 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash-16           28142             41585 ns/op            1720 B/op         56 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash1k-16         29500             39063 ns/op            1752 B/op         56 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist-16               281184              4451 ns/op             320 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1k-16             235394              4316 ns/op             334 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash-16             272566              4569 ns/op             312 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash1k-16           278380              4315 ns/op             326 B/op         13 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete-16                         20154             64004 ns/op            3664 B/op        108 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1k-16                       19230             61428 ns/op            3712 B/op        108 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash-16               18270             62749 ns/op            3632 B/op        108 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash1k-16             20653             60017 ns/op            3680 B/op        108 allocs/op
BenchmarkStorage/BenchmarkGradNonexist-16                         157063              7336 ns/op            1929 B/op         52 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1k-16                       161649              7598 ns/op            1945 B/op         52 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash-16               163005              7436 ns/op            1913 B/op         52 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash1k-16             151135              7524 ns/op            1929 B/op         52 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher-16                       70124             16813 ns/op           15277 B/op         83 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher1kInfoHash-16             71144             17011 ns/op           15261 B/op         83 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder-16                       149967              8055 ns/op            6814 B/op         42 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder1kInfoHash-16             141770              8202 ns/op            6806 B/op         42 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm-16                           55156             21739 ns/op            1120 B/op         41 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm1kInfoHash-16                 58994             21821 ns/op            1087 B/op         41 allocs/op
PASS
ok      github.com/sot-tech/mochi/storage/redis 43.235s
```

## KeyDB

Version: 6.3.4

Configuration: OOTB

```
goos: linux
goarch: amd64
pkg: github.com/sot-tech/mochi/storage/keydb
cpu: 12th Gen Intel(R) Core(TM) i5-12500H
BenchmarkStorage/BenchmarkNop-16                1000000000               0.1873 ns/op          0 B/op          0 allocs/op
BenchmarkStorage/BenchmarkPut-16                  141714              8824 ns/op             553 B/op         21 allocs/op
BenchmarkStorage/BenchmarkPut1k-16                141138              9215 ns/op             566 B/op         21 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash-16                113070              8939 ns/op             546 B/op         21 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash1k-16              134258              8578 ns/op             558 B/op         21 allocs/op
BenchmarkStorage/BenchmarkPutDelete-16                     31476             37899 ns/op             856 B/op         33 allocs/op
BenchmarkStorage/BenchmarkPutDelete1k-16                   34111             35877 ns/op             880 B/op         33 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash-16           31716             35344 ns/op             840 B/op         33 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash1k-16         33234             37156 ns/op             864 B/op         33 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist-16               292579              4302 ns/op             320 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1k-16             289604              4401 ns/op             334 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash-16             272372              4297 ns/op             312 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash1k-16           284304              4165 ns/op             326 B/op         13 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete-16                         23744             49850 ns/op            1304 B/op         48 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1k-16                       23378             53921 ns/op            1344 B/op         48 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash-16               25268             49024 ns/op            1272 B/op         48 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash1k-16             21583             50219 ns/op            1312 B/op         48 allocs/op
BenchmarkStorage/BenchmarkGradNonexist-16                         133311              8960 ns/op             669 B/op         24 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1k-16                       134439              8884 ns/op             683 B/op         24 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash-16               130093              9005 ns/op             653 B/op         24 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash1k-16             137900              8774 ns/op             667 B/op         24 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher-16                       75067             16103 ns/op           15276 B/op         83 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher1kInfoHash-16             72286             16315 ns/op           15261 B/op         83 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder-16                       165186              7705 ns/op            6814 B/op         42 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder1kInfoHash-16             151609              7799 ns/op            6806 B/op         42 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm-16                           54744             21244 ns/op            1120 B/op         41 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm1kInfoHash-16                 54865             21736 ns/op            1088 B/op         41 allocs/op
PASS
ok      github.com/sot-tech/mochi/storage/keydb 44.466s
```

## PostgreSQL

Version: 14.12

Configuration: OOTB

```
goos: linux
goarch: amd64
pkg: github.com/sot-tech/mochi/storage/pg
cpu: 12th Gen Intel(R) Core(TM) i5-12500H
BenchmarkStorage/BenchmarkNop-16                1000000000               0.1687 ns/op          0 B/op          0 allocs/op
BenchmarkStorage/BenchmarkPut-16                   58521             19857 ns/op            2213 B/op         44 allocs/op
BenchmarkStorage/BenchmarkPut1k-16                153538              7259 ns/op            2208 B/op         44 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash-16                151515              7319 ns/op            2204 B/op         44 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash1k-16              165950              7452 ns/op            2211 B/op         44 allocs/op
BenchmarkStorage/BenchmarkPutDelete-16                     17130             61564 ns/op            4274 B/op         81 allocs/op
BenchmarkStorage/BenchmarkPutDelete1k-16                   19200             60632 ns/op            4285 B/op         81 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash-16           19923             59570 ns/op            4273 B/op         81 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash1k-16         20510             61910 ns/op            4285 B/op         80 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist-16               184810              5485 ns/op            2098 B/op         37 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1k-16             187735              5514 ns/op            2108 B/op         37 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash-16             217992              5621 ns/op            2099 B/op         37 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash1k-16           215710              5569 ns/op            2109 B/op         37 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete-16                          8280            139091 ns/op            7306 B/op        143 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1k-16                        9010            133127 ns/op            7320 B/op        143 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash-16                8030            133810 ns/op            7305 B/op        143 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash1k-16              8217            136543 ns/op            7321 B/op        143 allocs/op
BenchmarkStorage/BenchmarkGradNonexist-16                          20883             56024 ns/op            3101 B/op         62 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1k-16                        20932             55863 ns/op            3106 B/op         62 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash-16                77368             15365 ns/op            3067 B/op         62 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash1k-16              65448             15568 ns/op            3081 B/op         62 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher-16                       55521             21614 ns/op           22565 B/op        380 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher1kInfoHash-16             54933             21813 ns/op           22571 B/op        380 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder-16                       117950             10443 ns/op            9239 B/op        190 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder1kInfoHash-16             113031             10490 ns/op            9242 B/op        190 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm-16                          103208             11945 ns/op            2951 B/op         46 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm1kInfoHash-16                101088             12110 ns/op            2952 B/op         46 allocs/op
PASS
ok      github.com/sot-tech/mochi/storage/pg    64.953s
```

## LMDB

Version: 0.9.31

```
goos: linux
goarch: amd64
pkg: github.com/sot-tech/mochi/storage/mdb
cpu: 12th Gen Intel(R) Core(TM) i5-12500H
BenchmarkStorage/BenchmarkNop-16                1000000000               0.1721 ns/op          0 B/op          0 allocs/op
BenchmarkStorage/BenchmarkPut-16                  327027              3127 ns/op             268 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPut1k-16                320313              4422 ns/op             268 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash-16                295910              4002 ns/op             260 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPut1kInfoHash1k-16              258867              4967 ns/op             260 B/op          6 allocs/op
BenchmarkStorage/BenchmarkPutDelete-16                    299650              3814 ns/op             568 B/op         13 allocs/op
BenchmarkStorage/BenchmarkPutDelete1k-16                  293272              4041 ns/op             568 B/op         13 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash-16          268844              4367 ns/op             552 B/op         13 allocs/op
BenchmarkStorage/BenchmarkPutDelete1kInfoHash1k-16        254877              4553 ns/op             552 B/op         13 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist-16               420516              4119 ns/op             372 B/op         11 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1k-16             218998              4668 ns/op             372 B/op         11 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash-16             308973              3916 ns/op             364 B/op         11 allocs/op
BenchmarkStorage/BenchmarkDeleteNonexist1kInfoHash1k-16           391814              3435 ns/op             364 B/op         11 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete-16                        152656              7754 ns/op             876 B/op         22 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1k-16                      153908              7915 ns/op             876 B/op         22 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash-16              130611              9273 ns/op             852 B/op         22 allocs/op
BenchmarkStorage/BenchmarkPutGradDelete1kInfoHash1k-16            127370              9124 ns/op             852 B/op         22 allocs/op
BenchmarkStorage/BenchmarkGradNonexist-16                         209017              5403 ns/op             380 B/op         13 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1k-16                       225026              6203 ns/op             380 B/op         13 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash-16               184981              6386 ns/op             372 B/op         13 allocs/op
BenchmarkStorage/BenchmarkGradNonexist1kInfoHash1k-16             181864              5550 ns/op             372 B/op         13 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher-16                      676172              1937 ns/op            3648 B/op         15 allocs/op
BenchmarkStorage/BenchmarkAnnounceLeecher1kInfoHash-16            639292              2022 ns/op            3636 B/op         15 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder-16                       926640              1300 ns/op            3416 B/op         10 allocs/op
BenchmarkStorage/BenchmarkAnnounceSeeder1kInfoHash-16             880938              1390 ns/op            3404 B/op         10 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm-16                         2990839               399.6 ns/op           344 B/op         13 allocs/op
BenchmarkStorage/BenchmarkScrapeSwarm1kInfoHash-16               3079238               382.5 ns/op           332 B/op         13 allocs/op
PASS
ok      github.com/sot-tech/mochi/storage/mdb   46.359s
```
