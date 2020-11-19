window.BENCHMARK_DATA = {
  "lastUpdate": 1605794440477,
  "repoUrl": "https://github.com/mlrun/storey",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "657577628afa1fbb26af178ad4557ba2c7e5d509",
          "message": "Add more benchmarks. (#20)\n\n* Add more benchmarks.\r\n\r\n* Better benchmarks.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-10-14T16:00:13+03:00",
          "tree_id": "1ce441b7e0b1ab1c38e1c06c9bcb93dcf263fbd2",
          "url": "https://github.com/mlrun/storey/commit/657577628afa1fbb26af178ad4557ba2c7e5d509"
        },
        "date": 1602680618974,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1117.2901185166188,
            "unit": "iter/sec",
            "range": "stddev: 0.00007117692377137618",
            "extra": "mean: 895.0226833901115 usec\nrounds: 578"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 713.6972502353996,
            "unit": "iter/sec",
            "range": "stddev: 0.0003779808083959806",
            "extra": "mean: 1.4011543405416915 msec\nrounds: 555"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.157794824213928,
            "unit": "iter/sec",
            "range": "stddev: 0.00840123239815128",
            "extra": "mean: 316.6766859999939 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6313227349913888,
            "unit": "iter/sec",
            "range": "stddev: 0.05700459447126654",
            "extra": "mean: 1.5839759041999968 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3434.319209179311,
            "unit": "iter/sec",
            "range": "stddev: 0.00005133746081838018",
            "extra": "mean: 291.1785245026676 usec\nrounds: 2061"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2385.665360984157,
            "unit": "iter/sec",
            "range": "stddev: 0.00010617017718138104",
            "extra": "mean: 419.1702727273832 usec\nrounds: 1881"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.123432145518352,
            "unit": "iter/sec",
            "range": "stddev: 0.002591235961475489",
            "extra": "mean: 89.90031016666933 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.230884358508166,
            "unit": "iter/sec",
            "range": "stddev: 0.0077510605921119755",
            "extra": "mean: 448.2527282000035 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 971.3106197089425,
            "unit": "iter/sec",
            "range": "stddev: 0.0005474349570327562",
            "extra": "mean: 1.0295367719748132 msec\nrounds: 785"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 669.6711152386642,
            "unit": "iter/sec",
            "range": "stddev: 0.0005873822650437438",
            "extra": "mean: 1.493270319182887 msec\nrounds: 636"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.6453414112437894,
            "unit": "iter/sec",
            "range": "stddev: 0.007098457905829306",
            "extra": "mean: 378.02303920000213 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.531932938479951,
            "unit": "iter/sec",
            "range": "stddev: 0.02034697801123083",
            "extra": "mean: 1.8799362244000064 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1066.435181794031,
            "unit": "iter/sec",
            "range": "stddev: 0.00009209482037622099",
            "extra": "mean: 937.7034976638064 usec\nrounds: 856"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 397.1533921201151,
            "unit": "iter/sec",
            "range": "stddev: 0.0008294443554020983",
            "extra": "mean: 2.5179188188768133 msec\nrounds: 392"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7547258087468914,
            "unit": "iter/sec",
            "range": "stddev: 0.006846319925187341",
            "extra": "mean: 1.3249845022000102 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1495195692642256,
            "unit": "iter/sec",
            "range": "stddev: 0.056703283088359195",
            "extra": "mean: 6.68808775280001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1082.2941566395525,
            "unit": "iter/sec",
            "range": "stddev: 0.00015006058621671876",
            "extra": "mean: 923.9632255845581 usec\nrounds: 727"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 676.9379112302706,
            "unit": "iter/sec",
            "range": "stddev: 0.0007799918985845292",
            "extra": "mean: 1.4772403545586545 msec\nrounds: 691"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.4226174221812546,
            "unit": "iter/sec",
            "range": "stddev: 0.003954064791707921",
            "extra": "mean: 292.17405180000924 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6838691505048222,
            "unit": "iter/sec",
            "range": "stddev: 0.014112728869019582",
            "extra": "mean: 1.4622680366000054 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8fe21463dba6e9a4d65d87e2e63d3aa986a197ce",
          "message": "Refactor persistence (#19)\n\n* refactor persistence to use v3io arrays\r\n\r\n* run conditional update after mtime cond evaluates to false\r\n\r\n* create a workaround for enging bug + fix lint + remove spaces in update expressions\r\n\r\n* break long lines\r\n\r\n* fix lint\r\n\r\n* code review fixes\r\n\r\n* replace double quates with single quates + make test parametarized\r\n\r\n* review fixes",
          "timestamp": "2020-10-15T14:39:54+03:00",
          "tree_id": "f05faa4b7f769690ab9e508b97917c512813f0ce",
          "url": "https://github.com/mlrun/storey/commit/8fe21463dba6e9a4d65d87e2e63d3aa986a197ce"
        },
        "date": 1602762219575,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1097.824634655155,
            "unit": "iter/sec",
            "range": "stddev: 0.00013389337192116078",
            "extra": "mean: 910.8922941177364 usec\nrounds: 340"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 665.4572155122935,
            "unit": "iter/sec",
            "range": "stddev: 0.0002529646679791538",
            "extra": "mean: 1.5027262109257664 msec\nrounds: 659"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.52200476394189,
            "unit": "iter/sec",
            "range": "stddev: 0.02260060560741257",
            "extra": "mean: 396.5099568000028 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5049428380273163,
            "unit": "iter/sec",
            "range": "stddev: 0.04140622526138037",
            "extra": "mean: 1.9804221878000021 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3475.3660503061524,
            "unit": "iter/sec",
            "range": "stddev: 0.00005252892024600168",
            "extra": "mean: 287.73947420931614 usec\nrounds: 2404"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2290.542907559454,
            "unit": "iter/sec",
            "range": "stddev: 0.00007941328962458165",
            "extra": "mean: 436.57771993692455 usec\nrounds: 1896"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 10.539370388169239,
            "unit": "iter/sec",
            "range": "stddev: 0.002141042301971389",
            "extra": "mean: 94.8823281818172 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.1053751434634513,
            "unit": "iter/sec",
            "range": "stddev: 0.009942206232760905",
            "extra": "mean: 474.9747346000049 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1086.0313020322542,
            "unit": "iter/sec",
            "range": "stddev: 0.00023749363913627396",
            "extra": "mean: 920.7837731092403 usec\nrounds: 714"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 659.3894064730937,
            "unit": "iter/sec",
            "range": "stddev: 0.0002641557700509702",
            "extra": "mean: 1.516554543010853 msec\nrounds: 744"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.4079538649793624,
            "unit": "iter/sec",
            "range": "stddev: 0.008875678814211483",
            "extra": "mean: 415.2903486000014 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.49552009120663293,
            "unit": "iter/sec",
            "range": "stddev: 0.03107654384904316",
            "extra": "mean: 2.018081643400001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 974.4387683003703,
            "unit": "iter/sec",
            "range": "stddev: 0.0003303154120311613",
            "extra": "mean: 1.0262317474747171 msec\nrounds: 396"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 364.525122736944,
            "unit": "iter/sec",
            "range": "stddev: 0.000607769469403425",
            "extra": "mean: 2.7432951465505444 msec\nrounds: 348"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.709930303738259,
            "unit": "iter/sec",
            "range": "stddev: 0.02291680483106593",
            "extra": "mean: 1.408588976599998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14142267527892194,
            "unit": "iter/sec",
            "range": "stddev: 0.04736792357024758",
            "extra": "mean: 7.071001860399986 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1083.8078975703806,
            "unit": "iter/sec",
            "range": "stddev: 0.0001389070536538002",
            "extra": "mean: 922.6727377072483 usec\nrounds: 488"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 651.895021624323,
            "unit": "iter/sec",
            "range": "stddev: 0.00026364562378961714",
            "extra": "mean: 1.5339893185689712 msec\nrounds: 587"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 2.7441061229785872,
            "unit": "iter/sec",
            "range": "stddev: 0.0026817583900674534",
            "extra": "mean: 364.4173931999944 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.547703637383912,
            "unit": "iter/sec",
            "range": "stddev: 0.021330715667904485",
            "extra": "mean: 1.825804927600018 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8573a472b5785d4a984c429478f4933940b19118",
          "message": "add aggregation related documentation (#22)\n\n* add aggregation related documentation\r\n\r\n* review comments\r\n\r\n* minor review fix",
          "timestamp": "2020-10-18T13:00:46+03:00",
          "tree_id": "9f0cae12fb889efcc45cc0aefb94262931ade870",
          "url": "https://github.com/mlrun/storey/commit/8573a472b5785d4a984c429478f4933940b19118"
        },
        "date": 1603015435736,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1271.2906205148429,
            "unit": "iter/sec",
            "range": "stddev: 0.00018017622428829995",
            "extra": "mean: 786.6022008366768 usec\nrounds: 478"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 837.9916360300243,
            "unit": "iter/sec",
            "range": "stddev: 0.0003507467427824947",
            "extra": "mean: 1.1933293329006105 msec\nrounds: 772"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.6119665987780984,
            "unit": "iter/sec",
            "range": "stddev: 0.006200310872736167",
            "extra": "mean: 276.8574881999996 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7563567698015289,
            "unit": "iter/sec",
            "range": "stddev: 0.01837535005078574",
            "extra": "mean: 1.322127387400002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4897.233989258138,
            "unit": "iter/sec",
            "range": "stddev: 0.00006692530007330289",
            "extra": "mean: 204.19690016720764 usec\nrounds: 2995"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3176.7429003659727,
            "unit": "iter/sec",
            "range": "stddev: 0.0000985906244720822",
            "extra": "mean: 314.7878287175195 usec\nrounds: 2347"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.545503428632111,
            "unit": "iter/sec",
            "range": "stddev: 0.004381482522471613",
            "extra": "mean: 73.82523693333005 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.8517466159800255,
            "unit": "iter/sec",
            "range": "stddev: 0.01052282154076737",
            "extra": "mean: 350.66229039999826 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1256.4988511574259,
            "unit": "iter/sec",
            "range": "stddev: 0.0005082602827997118",
            "extra": "mean: 795.8622477679534 usec\nrounds: 896"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 818.0800014159613,
            "unit": "iter/sec",
            "range": "stddev: 0.00012054713672296694",
            "extra": "mean: 1.2223743378021283 msec\nrounds: 746"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.3738638248720387,
            "unit": "iter/sec",
            "range": "stddev: 0.005112258580164949",
            "extra": "mean: 296.3960764000092 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6643087053086892,
            "unit": "iter/sec",
            "range": "stddev: 0.04510339441050078",
            "extra": "mean: 1.50532424459999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1142.9283908271411,
            "unit": "iter/sec",
            "range": "stddev: 0.00034513022912423193",
            "extra": "mean: 874.9454541734644 usec\nrounds: 982"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 418.90963366976615,
            "unit": "iter/sec",
            "range": "stddev: 0.0008860857347480032",
            "extra": "mean: 2.3871496848609537 msec\nrounds: 403"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8411445341501133,
            "unit": "iter/sec",
            "range": "stddev: 0.0321086835862927",
            "extra": "mean: 1.188856325400002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16825152076703703,
            "unit": "iter/sec",
            "range": "stddev: 0.04682069283441367",
            "extra": "mean: 5.943482682600006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1300.6124560744993,
            "unit": "iter/sec",
            "range": "stddev: 0.0001583146643442701",
            "extra": "mean: 768.8685398402181 usec\nrounds: 1117"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 806.6849699987548,
            "unit": "iter/sec",
            "range": "stddev: 0.000552130371218832",
            "extra": "mean: 1.239641293926108 msec\nrounds: 609"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.163968269488784,
            "unit": "iter/sec",
            "range": "stddev: 0.008739766549724914",
            "extra": "mean: 240.155528400021 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7647447505228915,
            "unit": "iter/sec",
            "range": "stddev: 0.0723057243137078",
            "extra": "mean: 1.3076258441999813 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ec4a2ca2dde8fa0bcd82e26a699e2969a73ae15d",
          "message": "Add dataframe source. (#21)\n\n* Add dataframe source.\r\n\r\n* Fix linter.\r\n\r\n* Code review round of improvements.\r\n\r\n* Use data in DataFrame index.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-10-18T15:08:47+03:00",
          "tree_id": "141e5565778d7741140eb173300373e2c6d71364",
          "url": "https://github.com/mlrun/storey/commit/ec4a2ca2dde8fa0bcd82e26a699e2969a73ae15d"
        },
        "date": 1603023134720,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1294.7240932749787,
            "unit": "iter/sec",
            "range": "stddev: 0.000047852677516872664",
            "extra": "mean: 772.3653287941218 usec\nrounds: 514"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 846.511120465394,
            "unit": "iter/sec",
            "range": "stddev: 0.00008017929952781399",
            "extra": "mean: 1.181319389460851 msec\nrounds: 778"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.228576726422461,
            "unit": "iter/sec",
            "range": "stddev: 0.0028100417620315486",
            "extra": "mean: 236.4861901999916 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.835195554802751,
            "unit": "iter/sec",
            "range": "stddev: 0.01545933642904874",
            "extra": "mean: 1.1973243802000013 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4937.728423661964,
            "unit": "iter/sec",
            "range": "stddev: 0.000014604578862252099",
            "extra": "mean: 202.52227627747308 usec\nrounds: 2407"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3329.1070754098328,
            "unit": "iter/sec",
            "range": "stddev: 0.000020431792228584083",
            "extra": "mean: 300.38084607924304 usec\nrounds: 2066"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.657382683081265,
            "unit": "iter/sec",
            "range": "stddev: 0.000825763315678846",
            "extra": "mean: 68.22500453333191 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.914856835943688,
            "unit": "iter/sec",
            "range": "stddev: 0.0030226485980689288",
            "extra": "mean: 343.070022400002 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1245.311019819044,
            "unit": "iter/sec",
            "range": "stddev: 0.00016136086731847514",
            "extra": "mean: 803.0122468082791 usec\nrounds: 705"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 765.5586215976356,
            "unit": "iter/sec",
            "range": "stddev: 0.00015036356010437926",
            "extra": "mean: 1.3062356974219838 msec\nrounds: 737"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.5037037792799524,
            "unit": "iter/sec",
            "range": "stddev: 0.006260164785399593",
            "extra": "mean: 285.41225599999507 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7125992293072847,
            "unit": "iter/sec",
            "range": "stddev: 0.02999537119368206",
            "extra": "mean: 1.4033133336000048 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1245.7122695265966,
            "unit": "iter/sec",
            "range": "stddev: 0.00007559202626938244",
            "extra": "mean: 802.7535928341031 usec\nrounds: 921"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 428.26523345114236,
            "unit": "iter/sec",
            "range": "stddev: 0.0008426699523010798",
            "extra": "mean: 2.3350015875479246 msec\nrounds: 257"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8970302095664147,
            "unit": "iter/sec",
            "range": "stddev: 0.01468300759704238",
            "extra": "mean: 1.1147896574000071 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17365344513784825,
            "unit": "iter/sec",
            "range": "stddev: 0.06811503682420289",
            "extra": "mean: 5.758595801000018 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1245.3629837164733,
            "unit": "iter/sec",
            "range": "stddev: 0.00026555768460232316",
            "extra": "mean: 802.9787403956321 usec\nrounds: 963"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 796.5421044455439,
            "unit": "iter/sec",
            "range": "stddev: 0.00046180514532475546",
            "extra": "mean: 1.2554264167819211 msec\nrounds: 715"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.26195984458997,
            "unit": "iter/sec",
            "range": "stddev: 0.009651713320948956",
            "extra": "mean: 234.6338390000028 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.872084464963665,
            "unit": "iter/sec",
            "range": "stddev: 0.011936187555909566",
            "extra": "mean: 1.1466779195999834 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "711560be222b2ca3d8c6062be9eb8880cea7b025",
          "message": "add sqr aggregations (#23)\n\n* add sqr aggregations\r\n\r\n* add sqr doc",
          "timestamp": "2020-10-18T22:13:15+03:00",
          "tree_id": "bdac923e8cc3924426b19e815b7036a0c0147722",
          "url": "https://github.com/mlrun/storey/commit/711560be222b2ca3d8c6062be9eb8880cea7b025"
        },
        "date": 1603048633064,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1294.4728117506688,
            "unit": "iter/sec",
            "range": "stddev: 0.00017883007306708956",
            "extra": "mean: 772.5152594341333 usec\nrounds: 424"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 840.8855109158212,
            "unit": "iter/sec",
            "range": "stddev: 0.00028550036223371004",
            "extra": "mean: 1.189222536265234 msec\nrounds: 841"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.8312775289408094,
            "unit": "iter/sec",
            "range": "stddev: 0.011429493488477722",
            "extra": "mean: 261.00954379999166 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7831682630971858,
            "unit": "iter/sec",
            "range": "stddev: 0.04045393923009433",
            "extra": "mean: 1.2768648158000075 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4949.247569749311,
            "unit": "iter/sec",
            "range": "stddev: 0.000014329585305937367",
            "extra": "mean: 202.05091499406484 usec\nrounds: 2541"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3334.2686077775247,
            "unit": "iter/sec",
            "range": "stddev: 0.00002132472089443485",
            "extra": "mean: 299.9158489113316 usec\nrounds: 2204"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.736179192424792,
            "unit": "iter/sec",
            "range": "stddev: 0.0014763748433510595",
            "extra": "mean: 67.86019543750221 msec\nrounds: 16"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.0259117812789613,
            "unit": "iter/sec",
            "range": "stddev: 0.0027848127900411256",
            "extra": "mean: 330.4789009999922 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1324.5916216502417,
            "unit": "iter/sec",
            "range": "stddev: 0.00009766010822531698",
            "extra": "mean: 754.9496642249258 usec\nrounds: 819"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 834.0374784286383,
            "unit": "iter/sec",
            "range": "stddev: 0.00011466616560314674",
            "extra": "mean: 1.198986887116922 msec\nrounds: 753"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.5873622920314445,
            "unit": "iter/sec",
            "range": "stddev: 0.005526445982063484",
            "extra": "mean: 278.7563447999901 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6712826826928154,
            "unit": "iter/sec",
            "range": "stddev: 0.049832162914629996",
            "extra": "mean: 1.4896853825999983 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1190.9339297331094,
            "unit": "iter/sec",
            "range": "stddev: 0.0002138822967391535",
            "extra": "mean: 839.6771433190269 usec\nrounds: 621"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 424.2083874602109,
            "unit": "iter/sec",
            "range": "stddev: 0.000802878778279671",
            "extra": "mean: 2.357331984846236 msec\nrounds: 264"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8145117124778074,
            "unit": "iter/sec",
            "range": "stddev: 0.021075636526624145",
            "extra": "mean: 1.2277294294000058 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16256722194415743,
            "unit": "iter/sec",
            "range": "stddev: 0.09043816234857878",
            "extra": "mean: 6.151301523399991 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1281.0174699223494,
            "unit": "iter/sec",
            "range": "stddev: 0.00022589614526367913",
            "extra": "mean: 780.629478894317 usec\nrounds: 687"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 805.9982241297962,
            "unit": "iter/sec",
            "range": "stddev: 0.00024142318034898056",
            "extra": "mean: 1.2406975227267525 msec\nrounds: 748"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.6388382739828096,
            "unit": "iter/sec",
            "range": "stddev: 0.01419370559412825",
            "extra": "mean: 274.81298279999464 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7059275240309029,
            "unit": "iter/sec",
            "range": "stddev: 0.09346179060989224",
            "extra": "mean: 1.416576016600004 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3ab4af7894e916d8c138d6abdf38708c93070429",
          "message": "Break flow.py into multiple files. (#24)\n\n* Break flow.py into multiple files.\r\n\r\n* Fix integration tests.\r\n\r\n* Extract writers from flow.py.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-10-19T14:48:04+03:00",
          "tree_id": "4869ccbab7ec8433d53337dc813fd5e022ca619d",
          "url": "https://github.com/mlrun/storey/commit/3ab4af7894e916d8c138d6abdf38708c93070429"
        },
        "date": 1603108282888,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1437.127304659381,
            "unit": "iter/sec",
            "range": "stddev: 0.00008179324650838057",
            "extra": "mean: 695.8325798680819 usec\nrounds: 457"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 905.9338181289186,
            "unit": "iter/sec",
            "range": "stddev: 0.00010742307286439592",
            "extra": "mean: 1.103833392670297 msec\nrounds: 955"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.257618596340569,
            "unit": "iter/sec",
            "range": "stddev: 0.009017563680860536",
            "extra": "mean: 234.87308160000566 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8398067260805626,
            "unit": "iter/sec",
            "range": "stddev: 0.025645040777885826",
            "extra": "mean: 1.1907501678000016 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5536.233669450053,
            "unit": "iter/sec",
            "range": "stddev: 0.00002691058019140334",
            "extra": "mean: 180.62821400010307 usec\nrounds: 2500"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3605.4061152542126,
            "unit": "iter/sec",
            "range": "stddev: 0.00003455276194210982",
            "extra": "mean: 277.36126473216774 usec\nrounds: 2240"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 15.574872858625616,
            "unit": "iter/sec",
            "range": "stddev: 0.0034154983889958753",
            "extra": "mean: 64.20598158823388 msec\nrounds: 17"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.2187074469775903,
            "unit": "iter/sec",
            "range": "stddev: 0.007589181516853742",
            "extra": "mean: 310.68371900000216 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1368.6198889237505,
            "unit": "iter/sec",
            "range": "stddev: 0.00010610961734905463",
            "extra": "mean: 730.6630629095823 usec\nrounds: 763"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 870.7376788083403,
            "unit": "iter/sec",
            "range": "stddev: 0.0001496021127912769",
            "extra": "mean: 1.1484515076556274 msec\nrounds: 849"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.7439213281483266,
            "unit": "iter/sec",
            "range": "stddev: 0.00646976487570129",
            "extra": "mean: 267.0996296000112 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7950998075192186,
            "unit": "iter/sec",
            "range": "stddev: 0.021420670683736726",
            "extra": "mean: 1.2577037379999978 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1330.773007443913,
            "unit": "iter/sec",
            "range": "stddev: 0.0000912024216797802",
            "extra": "mean: 751.4429541374254 usec\nrounds: 1003"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 456.06136398082083,
            "unit": "iter/sec",
            "range": "stddev: 0.0007392365585547187",
            "extra": "mean: 2.192687385906371 msec\nrounds: 298"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9840016923223265,
            "unit": "iter/sec",
            "range": "stddev: 0.01562054298327297",
            "extra": "mean: 1.0162584147999951 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.19220721164096702,
            "unit": "iter/sec",
            "range": "stddev: 0.047946092050408375",
            "extra": "mean: 5.202718417599999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1375.862891898813,
            "unit": "iter/sec",
            "range": "stddev: 0.00006777097992183497",
            "extra": "mean: 726.8166078815538 usec\nrounds: 1015"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 882.2205796672743,
            "unit": "iter/sec",
            "range": "stddev: 0.00009765148998395137",
            "extra": "mean: 1.1335033698456067 msec\nrounds: 776"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.6247795890167405,
            "unit": "iter/sec",
            "range": "stddev: 0.004932183525939327",
            "extra": "mean: 216.22652080001217 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9136149083578483,
            "unit": "iter/sec",
            "range": "stddev: 0.03405470182665542",
            "extra": "mean: 1.0945530669999926 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cf03414b5456214ac09a0a6fa5635e0ca5337bda",
          "message": "fix loading aggregation (#25)",
          "timestamp": "2020-10-19T19:44:45+03:00",
          "tree_id": "4722fbc1fb7f728711ea2ad677142013b25670e4",
          "url": "https://github.com/mlrun/storey/commit/cf03414b5456214ac09a0a6fa5635e0ca5337bda"
        },
        "date": 1603126090010,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1168.6393157761433,
            "unit": "iter/sec",
            "range": "stddev: 0.0005704597842299665",
            "extra": "mean: 855.696010309098 usec\nrounds: 582"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 831.7972053182802,
            "unit": "iter/sec",
            "range": "stddev: 0.00023261313902165172",
            "extra": "mean: 1.2022161094149846 msec\nrounds: 786"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.8833989489378307,
            "unit": "iter/sec",
            "range": "stddev: 0.0063834697522028045",
            "extra": "mean: 257.506378599993 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8048790245569266,
            "unit": "iter/sec",
            "range": "stddev: 0.016580596616696846",
            "extra": "mean: 1.2424227361999953 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4896.109095952786,
            "unit": "iter/sec",
            "range": "stddev: 0.000026064512864651555",
            "extra": "mean: 204.24381491552512 usec\nrounds: 2588"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3257.581176506211,
            "unit": "iter/sec",
            "range": "stddev: 0.00005787469616700382",
            "extra": "mean: 306.97623353549403 usec\nrounds: 2475"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.089827691936932,
            "unit": "iter/sec",
            "range": "stddev: 0.0005080727223466955",
            "extra": "mean: 70.9731887333343 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.816177324043156,
            "unit": "iter/sec",
            "range": "stddev: 0.0005800772403544425",
            "extra": "mean: 355.09127620000527 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1290.6976961517737,
            "unit": "iter/sec",
            "range": "stddev: 0.0002474041979213516",
            "extra": "mean: 774.7747617288762 usec\nrounds: 810"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 789.5964560052646,
            "unit": "iter/sec",
            "range": "stddev: 0.0003767489724978119",
            "extra": "mean: 1.2664697167705279 msec\nrounds: 805"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.537268910372346,
            "unit": "iter/sec",
            "range": "stddev: 0.0038570628105573787",
            "extra": "mean: 282.70398020000584 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7227343538491663,
            "unit": "iter/sec",
            "range": "stddev: 0.01935009048462406",
            "extra": "mean: 1.383634242200003 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1270.4615819201924,
            "unit": "iter/sec",
            "range": "stddev: 0.00004349547844158417",
            "extra": "mean: 787.1154974151889 usec\nrounds: 967"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 421.07880554524394,
            "unit": "iter/sec",
            "range": "stddev: 0.0010288644005821456",
            "extra": "mean: 2.3748523716484047 msec\nrounds: 261"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8887564995627111,
            "unit": "iter/sec",
            "range": "stddev: 0.016260806734524128",
            "extra": "mean: 1.1251675802000023 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1775699529984517,
            "unit": "iter/sec",
            "range": "stddev: 0.020397261942012194",
            "extra": "mean: 5.631583401999995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1336.8236298789611,
            "unit": "iter/sec",
            "range": "stddev: 0.000049872247760828665",
            "extra": "mean: 748.0418341277691 usec\nrounds: 1049"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 836.4602764446977,
            "unit": "iter/sec",
            "range": "stddev: 0.00007182283296287975",
            "extra": "mean: 1.195514034749401 msec\nrounds: 777"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.110637749251056,
            "unit": "iter/sec",
            "range": "stddev: 0.010215552984739",
            "extra": "mean: 243.27125399998977 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.875479212851474,
            "unit": "iter/sec",
            "range": "stddev: 0.019403372585866215",
            "extra": "mean: 1.142231574799996 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "94abeaa4415a00dc82cf9d110a29456390f2faf2",
          "message": "save aggr time when loading an aggregation (#26)\n\n* save aggr time when loading an aggregation\r\n\r\n* add tests + fix bugs\r\n\r\n* don't pop initial data\r\n\r\n* change to test to actually test 1 window + rename tests",
          "timestamp": "2020-10-22T11:28:26+03:00",
          "tree_id": "c7423f42887ed7bdec1dd284ce0797b9f514b5d2",
          "url": "https://github.com/mlrun/storey/commit/94abeaa4415a00dc82cf9d110a29456390f2faf2"
        },
        "date": 1603355560994,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1165.096421001706,
            "unit": "iter/sec",
            "range": "stddev: 0.0003859594443468905",
            "extra": "mean: 858.2980618378672 usec\nrounds: 566"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 753.1591284312436,
            "unit": "iter/sec",
            "range": "stddev: 0.00015338242685505115",
            "extra": "mean: 1.3277406622992163 msec\nrounds: 687"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.958917770806892,
            "unit": "iter/sec",
            "range": "stddev: 0.021148520604555902",
            "extra": "mean: 337.96140260001266 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6501915884053222,
            "unit": "iter/sec",
            "range": "stddev: 0.030285022323057353",
            "extra": "mean: 1.5380082083999695 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4283.647135787445,
            "unit": "iter/sec",
            "range": "stddev: 0.00009973391976644109",
            "extra": "mean: 233.44593247318772 usec\nrounds: 1866"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2893.425063043567,
            "unit": "iter/sec",
            "range": "stddev: 0.00006706697128443046",
            "extra": "mean: 345.61116262264943 usec\nrounds: 2183"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 12.49625528511342,
            "unit": "iter/sec",
            "range": "stddev: 0.006388976019507539",
            "extra": "mean: 80.0239733571451 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.555942470037653,
            "unit": "iter/sec",
            "range": "stddev: 0.012540764643488371",
            "extra": "mean: 391.24511279992475 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1016.8008185584221,
            "unit": "iter/sec",
            "range": "stddev: 0.00035609497972219405",
            "extra": "mean: 983.4767849791451 usec\nrounds: 679"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 679.2830993013355,
            "unit": "iter/sec",
            "range": "stddev: 0.00048783068556407824",
            "extra": "mean: 1.472140262327345 msec\nrounds: 568"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.630821269458024,
            "unit": "iter/sec",
            "range": "stddev: 0.023239363720746194",
            "extra": "mean: 380.1094402000217 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5405966445954057,
            "unit": "iter/sec",
            "range": "stddev: 0.041713784407944925",
            "extra": "mean: 1.8498080037999898 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1167.7218508384149,
            "unit": "iter/sec",
            "range": "stddev: 0.0000882043743296227",
            "extra": "mean: 856.3683203170413 usec\nrounds: 871"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 380.10873625628136,
            "unit": "iter/sec",
            "range": "stddev: 0.0007820633628928828",
            "extra": "mean: 2.63082614161693 msec\nrounds: 346"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7202809961029044,
            "unit": "iter/sec",
            "range": "stddev: 0.015138637831419582",
            "extra": "mean: 1.388347055399936 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14003110763105875,
            "unit": "iter/sec",
            "range": "stddev: 0.039484122549143956",
            "extra": "mean: 7.141270371399969 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1095.4670526975458,
            "unit": "iter/sec",
            "range": "stddev: 0.0005964015206558025",
            "extra": "mean: 912.8526481353667 usec\nrounds: 1043"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 732.9446197954015,
            "unit": "iter/sec",
            "range": "stddev: 0.00019953256938245423",
            "extra": "mean: 1.3643595614074442 msec\nrounds: 627"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.4397196069913107,
            "unit": "iter/sec",
            "range": "stddev: 0.006885020178370621",
            "extra": "mean: 290.72137099997235 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6904209623948929,
            "unit": "iter/sec",
            "range": "stddev: 0.03948798623456343",
            "extra": "mean: 1.4483917124000072 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e47cc9a1dd32dc022fef3768bf47a136f6a792f0",
          "message": "Add ToDataFrame sink. (#27)\n\n* Add ToDataFrame sink.\r\n\r\n* Docu. Extra test.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-10-26T10:39:21+02:00",
          "tree_id": "9fd02e8acc44f7c49ef3c06a17c1f059c51e4d02",
          "url": "https://github.com/mlrun/storey/commit/e47cc9a1dd32dc022fef3768bf47a136f6a792f0"
        },
        "date": 1603701777349,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1282.970317674116,
            "unit": "iter/sec",
            "range": "stddev: 0.000057865041941389586",
            "extra": "mean: 779.4412592591308 usec\nrounds: 621"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 822.1580527682183,
            "unit": "iter/sec",
            "range": "stddev: 0.00007685062489930403",
            "extra": "mean: 1.2163111419184975 msec\nrounds: 761"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.960746931978405,
            "unit": "iter/sec",
            "range": "stddev: 0.0024316100245527703",
            "extra": "mean: 252.47763040000564 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8021426199651045,
            "unit": "iter/sec",
            "range": "stddev: 0.010217152203915688",
            "extra": "mean: 1.24666109879999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4766.392932794035,
            "unit": "iter/sec",
            "range": "stddev: 0.000009067357147496654",
            "extra": "mean: 209.802258038723 usec\nrounds: 2488"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3190.758060462409,
            "unit": "iter/sec",
            "range": "stddev: 0.000009990365370083385",
            "extra": "mean: 313.40514731946763 usec\nrounds: 2145"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.615151526458785,
            "unit": "iter/sec",
            "range": "stddev: 0.00031056439759412043",
            "extra": "mean: 73.44758507143061 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.732299498126358,
            "unit": "iter/sec",
            "range": "stddev: 0.0006812846046257943",
            "extra": "mean: 365.9920886000009 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1272.1797046803702,
            "unit": "iter/sec",
            "range": "stddev: 0.00007627067200871446",
            "extra": "mean: 786.0524706698146 usec\nrounds: 716"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 795.77309427425,
            "unit": "iter/sec",
            "range": "stddev: 0.00007155843864278069",
            "extra": "mean: 1.2566396215142284 msec\nrounds: 753"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.5320823647021724,
            "unit": "iter/sec",
            "range": "stddev: 0.00523076211074574",
            "extra": "mean: 283.1191055999966 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7031100014881574,
            "unit": "iter/sec",
            "range": "stddev: 0.033933994668915876",
            "extra": "mean: 1.4222525605999976 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1226.8947923415028,
            "unit": "iter/sec",
            "range": "stddev: 0.00017578776455874054",
            "extra": "mean: 815.065811870895 usec\nrounds: 994"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 419.2740428639715,
            "unit": "iter/sec",
            "range": "stddev: 0.0006809806154492218",
            "extra": "mean: 2.3850749098828383 msec\nrounds: 344"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8390824783865184,
            "unit": "iter/sec",
            "range": "stddev: 0.004049377244216979",
            "extra": "mean: 1.1917779547999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16398808417909982,
            "unit": "iter/sec",
            "range": "stddev: 0.03163928570491998",
            "extra": "mean: 6.098004041000007 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1275.0687524576927,
            "unit": "iter/sec",
            "range": "stddev: 0.00019198355265691324",
            "extra": "mean: 784.271434832437 usec\nrounds: 867"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 812.3346628749398,
            "unit": "iter/sec",
            "range": "stddev: 0.00006455640661870096",
            "extra": "mean: 1.2310197332474924 msec\nrounds: 776"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.322441104492332,
            "unit": "iter/sec",
            "range": "stddev: 0.0024384516282045174",
            "extra": "mean: 231.35075199999733 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8617488893959522,
            "unit": "iter/sec",
            "range": "stddev: 0.01725607108653306",
            "extra": "mean: 1.1604308544000048 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "38646ff687af3623ad39cc50fdea01ef6c8051dd",
          "message": "Fix teardown of WriteCSV tests. (#30)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-10-26T14:04:34+02:00",
          "tree_id": "acd97a81dd1247ca451f205bd6ad4e9038b6ba1f",
          "url": "https://github.com/mlrun/storey/commit/38646ff687af3623ad39cc50fdea01ef6c8051dd"
        },
        "date": 1603714091270,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1273.1610220579316,
            "unit": "iter/sec",
            "range": "stddev: 0.0005645589848317338",
            "extra": "mean: 785.446603119851 usec\nrounds: 577"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 867.9434108197602,
            "unit": "iter/sec",
            "range": "stddev: 0.00015717836405256983",
            "extra": "mean: 1.1521488469571008 msec\nrounds: 575"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.762352027447235,
            "unit": "iter/sec",
            "range": "stddev: 0.004988052528068146",
            "extra": "mean: 265.7911839999997 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7298364528147468,
            "unit": "iter/sec",
            "range": "stddev: 0.0855656762899832",
            "extra": "mean: 1.3701699828000073 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5280.084767446432,
            "unit": "iter/sec",
            "range": "stddev: 0.00005672998912627222",
            "extra": "mean: 189.39089882900166 usec\nrounds: 3074"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3542.1222667707184,
            "unit": "iter/sec",
            "range": "stddev: 0.00005768952598619723",
            "extra": "mean: 282.3166239576704 usec\nrounds: 2638"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.801580146387982,
            "unit": "iter/sec",
            "range": "stddev: 0.0034761914483809556",
            "extra": "mean: 67.5603543750043 msec\nrounds: 16"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.038080763787294,
            "unit": "iter/sec",
            "range": "stddev: 0.012663683208814747",
            "extra": "mean: 329.1551732000016 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1286.54701778171,
            "unit": "iter/sec",
            "range": "stddev: 0.00021582619574310723",
            "extra": "mean: 777.2743523390384 usec\nrounds: 684"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 784.3379395530965,
            "unit": "iter/sec",
            "range": "stddev: 0.0003052329783233477",
            "extra": "mean: 1.2749606382292107 msec\nrounds: 926"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.9714498904778175,
            "unit": "iter/sec",
            "range": "stddev: 0.017060016187005235",
            "extra": "mean: 336.53604700000415 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.610074534520192,
            "unit": "iter/sec",
            "range": "stddev: 0.027094695591703535",
            "extra": "mean: 1.6391439789999993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1281.3609256394234,
            "unit": "iter/sec",
            "range": "stddev: 0.00012817404074662496",
            "extra": "mean: 780.420239130502 usec\nrounds: 1104"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 415.38132758477303,
            "unit": "iter/sec",
            "range": "stddev: 0.000950591153442685",
            "extra": "mean: 2.4074264623652715 msec\nrounds: 372"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8374591339602022,
            "unit": "iter/sec",
            "range": "stddev: 0.02162406301205539",
            "extra": "mean: 1.194088116599994 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17130536326853918,
            "unit": "iter/sec",
            "range": "stddev: 0.009157452804586122",
            "extra": "mean: 5.837528848599999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1253.1871966213694,
            "unit": "iter/sec",
            "range": "stddev: 0.00030410174966296363",
            "extra": "mean: 797.9653819445572 usec\nrounds: 1008"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 805.5422266584942,
            "unit": "iter/sec",
            "range": "stddev: 0.0006548589099085962",
            "extra": "mean: 1.2413998508161947 msec\nrounds: 858"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.837910566326041,
            "unit": "iter/sec",
            "range": "stddev: 0.0026298953180306016",
            "extra": "mean: 260.5584426000007 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7871609693218322,
            "unit": "iter/sec",
            "range": "stddev: 0.03127714443565843",
            "extra": "mean: 1.2703881911999986 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d669133b5e59dccb36258fc04a7a977875fde665",
          "message": "dont save schema on QueryAggregateByKey + use getSafeEvent in joinWithV3io (#29)\n\n* dont save schema on QueryAggregateByKey + use getSafeEvent in joinWithV3io\r\n\r\n* fix test\r\n\r\n* amek read_only private in aggregate_store",
          "timestamp": "2020-10-26T16:04:43+02:00",
          "tree_id": "5fa054f92136193c3d5b70cbdca8e810645eb7b2",
          "url": "https://github.com/mlrun/storey/commit/d669133b5e59dccb36258fc04a7a977875fde665"
        },
        "date": 1603721328684,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1202.4206109659171,
            "unit": "iter/sec",
            "range": "stddev: 0.00015194016538999506",
            "extra": "mean: 831.6557375016131 usec\nrounds: 320"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 814.3044016093464,
            "unit": "iter/sec",
            "range": "stddev: 0.0002277513848846707",
            "extra": "mean: 1.2280419926794637 msec\nrounds: 683"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.878402449263845,
            "unit": "iter/sec",
            "range": "stddev: 0.022649693127362612",
            "extra": "mean: 257.8381210000032 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.71361616544766,
            "unit": "iter/sec",
            "range": "stddev: 0.04884226369788265",
            "extra": "mean: 1.4013135470000009 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4326.625064376997,
            "unit": "iter/sec",
            "range": "stddev: 0.00009350029269433167",
            "extra": "mean: 231.12702975661998 usec\nrounds: 2218"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3018.473178201136,
            "unit": "iter/sec",
            "range": "stddev: 0.00009650448280208601",
            "extra": "mean: 331.2933198220273 usec\nrounds: 2023"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.699788479571662,
            "unit": "iter/sec",
            "range": "stddev: 0.0017588066965485987",
            "extra": "mean: 72.99382771428498 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.714881773870585,
            "unit": "iter/sec",
            "range": "stddev: 0.00808959432951005",
            "extra": "mean: 368.3401647999972 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 980.4358696375449,
            "unit": "iter/sec",
            "range": "stddev: 0.00043001861196113243",
            "extra": "mean: 1.0199545232567713 msec\nrounds: 86"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 679.6081821950019,
            "unit": "iter/sec",
            "range": "stddev: 0.00044248890060934426",
            "extra": "mean: 1.4714360806695927 msec\nrounds: 657"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.9055873953405364,
            "unit": "iter/sec",
            "range": "stddev: 0.011473991473770314",
            "extra": "mean: 344.1644886000063 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5887734986917488,
            "unit": "iter/sec",
            "range": "stddev: 0.05850074493021843",
            "extra": "mean: 1.6984460105999915 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1195.2697029260182,
            "unit": "iter/sec",
            "range": "stddev: 0.00019309802995640923",
            "extra": "mean: 836.6312620089021 usec\nrounds: 916"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 392.18870877830506,
            "unit": "iter/sec",
            "range": "stddev: 0.0011270107041952927",
            "extra": "mean: 2.549792937984036 msec\nrounds: 387"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8088492399576118,
            "unit": "iter/sec",
            "range": "stddev: 0.032853700143827276",
            "extra": "mean: 1.2363243365999892 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16356878702992025,
            "unit": "iter/sec",
            "range": "stddev: 0.14667041561443034",
            "extra": "mean: 6.113635848000013 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1239.9973686882029,
            "unit": "iter/sec",
            "range": "stddev: 0.0002917909320366227",
            "extra": "mean: 806.4533242178595 usec\nrounds: 1024"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 789.0817967583123,
            "unit": "iter/sec",
            "range": "stddev: 0.00030494691291597047",
            "extra": "mean: 1.2672957405787044 msec\nrounds: 690"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.384449215878483,
            "unit": "iter/sec",
            "range": "stddev: 0.01583740466081739",
            "extra": "mean: 228.07881920001591 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7644279270146612,
            "unit": "iter/sec",
            "range": "stddev: 0.02812853718209832",
            "extra": "mean: 1.3081678006000175 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fe972539c113cd033e8533b3f4b99d18383915a1",
          "message": "Add ToDataFrame step, rename existing ToDataFrame as ReduceToDataFrame. (#31)\n\n* Add ToDataFrame step, rename existing ToDataFrame as ReduceToDataFrame.\r\n\r\n* Remove unused code.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-10-27T14:30:49+02:00",
          "tree_id": "d22879ea8399b3ad05086c049a5725e2c33fed23",
          "url": "https://github.com/mlrun/storey/commit/fe972539c113cd033e8533b3f4b99d18383915a1"
        },
        "date": 1603802096138,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1131.6267183258517,
            "unit": "iter/sec",
            "range": "stddev: 0.00009531042852426634",
            "extra": "mean: 883.6836244724033 usec\nrounds: 474"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 722.0369762986672,
            "unit": "iter/sec",
            "range": "stddev: 0.0004211442183361658",
            "extra": "mean: 1.3849706217626652 msec\nrounds: 579"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.10092586129575,
            "unit": "iter/sec",
            "range": "stddev: 0.005795236755009639",
            "extra": "mean: 322.48433039999895 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6249202463502056,
            "unit": "iter/sec",
            "range": "stddev: 0.014522534250840782",
            "extra": "mean: 1.6002041953999993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3598.105228048009,
            "unit": "iter/sec",
            "range": "stddev: 0.00002539791281256478",
            "extra": "mean: 277.92405630740967 usec\nrounds: 2291"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2452.386093538698,
            "unit": "iter/sec",
            "range": "stddev: 0.00003829743967072369",
            "extra": "mean: 407.76613545261085 usec\nrounds: 2045"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.417344297230475,
            "unit": "iter/sec",
            "range": "stddev: 0.00043707635548137383",
            "extra": "mean: 87.58604224999782 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.2672073082466238,
            "unit": "iter/sec",
            "range": "stddev: 0.0066551371769279334",
            "extra": "mean: 441.07126700000094 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1131.9299423687642,
            "unit": "iter/sec",
            "range": "stddev: 0.0003245522237117362",
            "extra": "mean: 883.4469012342961 usec\nrounds: 810"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 715.0133711334961,
            "unit": "iter/sec",
            "range": "stddev: 0.0001662615202715062",
            "extra": "mean: 1.398575244005186 msec\nrounds: 709"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.646608754321973,
            "unit": "iter/sec",
            "range": "stddev: 0.016501262808322884",
            "extra": "mean: 377.8420208000057 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5339362728532874,
            "unit": "iter/sec",
            "range": "stddev: 0.08200530874695759",
            "extra": "mean: 1.8728826844000082 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1094.0570255868515,
            "unit": "iter/sec",
            "range": "stddev: 0.00017456968502559112",
            "extra": "mean: 914.0291379817252 usec\nrounds: 674"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 387.8187646951106,
            "unit": "iter/sec",
            "range": "stddev: 0.0010230599789751386",
            "extra": "mean: 2.578524019553733 msec\nrounds: 358"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7785749318605366,
            "unit": "iter/sec",
            "range": "stddev: 0.008190617632782518",
            "extra": "mean: 1.284397890400004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1540977317784483,
            "unit": "iter/sec",
            "range": "stddev: 0.04793621480736654",
            "extra": "mean: 6.4893881853999975 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1153.098659657181,
            "unit": "iter/sec",
            "range": "stddev: 0.00011212154296916435",
            "extra": "mean: 867.2284818172474 usec\nrounds: 880"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 722.7244043672563,
            "unit": "iter/sec",
            "range": "stddev: 0.0001394381690980199",
            "extra": "mean: 1.383653290185348 msec\nrounds: 703"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.297433615199183,
            "unit": "iter/sec",
            "range": "stddev: 0.008180414941857107",
            "extra": "mean: 303.26615080000465 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6395566375632181,
            "unit": "iter/sec",
            "range": "stddev: 0.03297234877075938",
            "extra": "mean: 1.563583178199997 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1153c023523cde0406eec9f34f8c8277015c4a83",
          "message": "Add MapClass. (#34)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-01T15:10:09+02:00",
          "tree_id": "95cefc31b6e30a28eac228ba265fb64f53896a02",
          "url": "https://github.com/mlrun/storey/commit/1153c023523cde0406eec9f34f8c8277015c4a83"
        },
        "date": 1604236419123,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1400.2561370263554,
            "unit": "iter/sec",
            "range": "stddev: 0.000059966074745171405",
            "extra": "mean: 714.1550560339933 usec\nrounds: 464"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 893.0145018363331,
            "unit": "iter/sec",
            "range": "stddev: 0.0001097144549055116",
            "extra": "mean: 1.1198026436789879 msec\nrounds: 783"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.186419887708226,
            "unit": "iter/sec",
            "range": "stddev: 0.007704147325577884",
            "extra": "mean: 238.8675830000011 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8211366708028747,
            "unit": "iter/sec",
            "range": "stddev: 0.031909800865640665",
            "extra": "mean: 1.2178240670000036 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5471.056867973543,
            "unit": "iter/sec",
            "range": "stddev: 0.000023620147894039054",
            "extra": "mean: 182.7800412483001 usec\nrounds: 2788"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3570.6280592810426,
            "unit": "iter/sec",
            "range": "stddev: 0.00003090250704503025",
            "extra": "mean: 280.062774222794 usec\nrounds: 2250"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 15.710568884135405,
            "unit": "iter/sec",
            "range": "stddev: 0.0020338670954341944",
            "extra": "mean: 63.651418823528665 msec\nrounds: 17"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.1906597412912143,
            "unit": "iter/sec",
            "range": "stddev: 0.0008640987925546846",
            "extra": "mean: 313.4148047999986 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1401.9357314989234,
            "unit": "iter/sec",
            "range": "stddev: 0.0001351440994499611",
            "extra": "mean: 713.2994598338818 usec\nrounds: 722"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 866.8708389840295,
            "unit": "iter/sec",
            "range": "stddev: 0.00009151422734404479",
            "extra": "mean: 1.15357439081928 msec\nrounds: 806"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.8165800335440068,
            "unit": "iter/sec",
            "range": "stddev: 0.004584550921428092",
            "extra": "mean: 262.0146809999994 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7704393096085168,
            "unit": "iter/sec",
            "range": "stddev: 0.011974576094674685",
            "extra": "mean: 1.2979607705999967 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1323.5998644383546,
            "unit": "iter/sec",
            "range": "stddev: 0.00011646932153815126",
            "extra": "mean: 755.5153387873243 usec\nrounds: 1039"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 456.7521951753041,
            "unit": "iter/sec",
            "range": "stddev: 0.0010511910447487068",
            "extra": "mean: 2.189370977442581 msec\nrounds: 399"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.937477302678437,
            "unit": "iter/sec",
            "range": "stddev: 0.00648893932315864",
            "extra": "mean: 1.066692491799995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.18282774693692494,
            "unit": "iter/sec",
            "range": "stddev: 0.04423502381441886",
            "extra": "mean: 5.469629291799987 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1372.8020959983974,
            "unit": "iter/sec",
            "range": "stddev: 0.00006209658490020658",
            "extra": "mean: 728.4371162565354 usec\nrounds: 1015"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 858.6429160014692,
            "unit": "iter/sec",
            "range": "stddev: 0.00021643924527594917",
            "extra": "mean: 1.1646284868415417 msec\nrounds: 912"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.471805790376031,
            "unit": "iter/sec",
            "range": "stddev: 0.0035704930463624656",
            "extra": "mean: 223.62330720000045 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9181819689973804,
            "unit": "iter/sec",
            "range": "stddev: 0.012120578432561997",
            "extra": "mean: 1.0891087319999997 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "04f6e901c4e025e958b3abe65a869b0229852db6",
          "message": "Add context attribute to Flow. (#33)\n\n* Add context attribute to Flow.\r\n\r\n* Make context and name public attributes.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-01T15:10:19+02:00",
          "tree_id": "54de3b8ae431ca0283498001a3a47a06596bcb93",
          "url": "https://github.com/mlrun/storey/commit/04f6e901c4e025e958b3abe65a869b0229852db6"
        },
        "date": 1604236475103,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1227.7641064467862,
            "unit": "iter/sec",
            "range": "stddev: 0.00025989591354896836",
            "extra": "mean: 814.48870735768 usec\nrounds: 598"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 801.1218442529685,
            "unit": "iter/sec",
            "range": "stddev: 0.0002348791065134633",
            "extra": "mean: 1.2482495729878416 msec\nrounds: 733"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.530041617157376,
            "unit": "iter/sec",
            "range": "stddev: 0.012927507924009705",
            "extra": "mean: 283.2827791999989 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7063131623891596,
            "unit": "iter/sec",
            "range": "stddev: 0.015219012061688766",
            "extra": "mean: 1.4158025834000056 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4553.2037637741905,
            "unit": "iter/sec",
            "range": "stddev: 0.000028760174981437416",
            "extra": "mean: 219.62557616158415 usec\nrounds: 2324"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3102.343917502596,
            "unit": "iter/sec",
            "range": "stddev: 0.00001302446877834793",
            "extra": "mean: 322.33692543185396 usec\nrounds: 2025"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.492655131986858,
            "unit": "iter/sec",
            "range": "stddev: 0.00010848824605480822",
            "extra": "mean: 74.11439707143431 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.71192383891236,
            "unit": "iter/sec",
            "range": "stddev: 0.00047726296885257027",
            "extra": "mean: 368.74191879999785 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1210.985221952549,
            "unit": "iter/sec",
            "range": "stddev: 0.00019234299864371313",
            "extra": "mean: 825.773908609418 usec\nrounds: 755"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 764.0328133375622,
            "unit": "iter/sec",
            "range": "stddev: 0.0003423315572304191",
            "extra": "mean: 1.3088443094893407 msec\nrounds: 685"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.3033497787562833,
            "unit": "iter/sec",
            "range": "stddev: 0.0027052367176398154",
            "extra": "mean: 302.7230136000014 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.664132079639254,
            "unit": "iter/sec",
            "range": "stddev: 0.028887422039558557",
            "extra": "mean: 1.5057245849999958 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1194.8122136146471,
            "unit": "iter/sec",
            "range": "stddev: 0.00007098293239903811",
            "extra": "mean: 836.9516051185276 usec\nrounds: 547"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 397.6133059807107,
            "unit": "iter/sec",
            "range": "stddev: 0.0014842642761847744",
            "extra": "mean: 2.515006376694327 msec\nrounds: 369"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.805842531772666,
            "unit": "iter/sec",
            "range": "stddev: 0.0366225240962958",
            "extra": "mean: 1.240937230999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1594090264563739,
            "unit": "iter/sec",
            "range": "stddev: 0.06690333096785954",
            "extra": "mean: 6.2731704862000015 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1236.1566616838024,
            "unit": "iter/sec",
            "range": "stddev: 0.00010889998822892106",
            "extra": "mean: 808.9589539871694 usec\nrounds: 978"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 797.6663212514203,
            "unit": "iter/sec",
            "range": "stddev: 0.00010014850907433567",
            "extra": "mean: 1.2536570409932164 msec\nrounds: 805"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.705700499830727,
            "unit": "iter/sec",
            "range": "stddev: 0.004319607939451753",
            "extra": "mean: 269.85451200000625 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7577175717668878,
            "unit": "iter/sec",
            "range": "stddev: 0.03270103248740236",
            "extra": "mean: 1.319752949199983 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8125f684313300cc4003877148a9495cb5eb9764",
          "message": "Add WriteToParquet. (#32)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-01T15:22:33+02:00",
          "tree_id": "ff66b9bba98295779c25490d372b09bd84852dd6",
          "url": "https://github.com/mlrun/storey/commit/8125f684313300cc4003877148a9495cb5eb9764"
        },
        "date": 1604237177899,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1316.1596621561064,
            "unit": "iter/sec",
            "range": "stddev: 0.00011752645701163628",
            "extra": "mean: 759.7862392787665 usec\nrounds: 443"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 802.409982660166,
            "unit": "iter/sec",
            "range": "stddev: 0.0002439140631455249",
            "extra": "mean: 1.2462457118053036 msec\nrounds: 576"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.2785749268061672,
            "unit": "iter/sec",
            "range": "stddev: 0.0103784847941591",
            "extra": "mean: 305.01056780000226 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6486222538039769,
            "unit": "iter/sec",
            "range": "stddev: 0.010731050477877887",
            "extra": "mean: 1.5417294028000073 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4104.501847791697,
            "unit": "iter/sec",
            "range": "stddev: 0.00005016895602591308",
            "extra": "mean: 243.63492503676656 usec\nrounds: 2708"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2842.5286159192087,
            "unit": "iter/sec",
            "range": "stddev: 0.00006109679042364014",
            "extra": "mean: 351.79944870198705 usec\nrounds: 2427"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.087714569783108,
            "unit": "iter/sec",
            "range": "stddev: 0.002381160890464544",
            "extra": "mean: 76.40753430769328 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.614546150706861,
            "unit": "iter/sec",
            "range": "stddev: 0.008370609592133553",
            "extra": "mean: 382.47555880000164 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1313.7203744821531,
            "unit": "iter/sec",
            "range": "stddev: 0.00014106560928813506",
            "extra": "mean: 761.1969939905846 usec\nrounds: 832"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 818.3550276925238,
            "unit": "iter/sec",
            "range": "stddev: 0.0003429755863297282",
            "extra": "mean: 1.221963531915545 msec\nrounds: 799"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.148967276059934,
            "unit": "iter/sec",
            "range": "stddev: 0.011905684743954849",
            "extra": "mean: 317.5644305999981 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5860018851258733,
            "unit": "iter/sec",
            "range": "stddev: 0.03397321736103109",
            "extra": "mean: 1.7064791519999971 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1199.2990064560677,
            "unit": "iter/sec",
            "range": "stddev: 0.00014869194629330194",
            "extra": "mean: 833.8204189420645 usec\nrounds: 1172"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 465.5425480998285,
            "unit": "iter/sec",
            "range": "stddev: 0.0008726447570125145",
            "extra": "mean: 2.148031375610303 msec\nrounds: 410"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9006546541290159,
            "unit": "iter/sec",
            "range": "stddev: 0.030643001592889924",
            "extra": "mean: 1.1103034836000005 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17063256538427615,
            "unit": "iter/sec",
            "range": "stddev: 0.3010022069980963",
            "extra": "mean: 5.860546008600011 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1209.6364230128925,
            "unit": "iter/sec",
            "range": "stddev: 0.00015695073185244114",
            "extra": "mean: 826.6946836052257 usec\nrounds: 1043"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 788.8451094778975,
            "unit": "iter/sec",
            "range": "stddev: 0.00037888348791594137",
            "extra": "mean: 1.2676759835170388 msec\nrounds: 910"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.3692733315294356,
            "unit": "iter/sec",
            "range": "stddev: 0.017697389862056018",
            "extra": "mean: 296.79990360000374 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.680492445538773,
            "unit": "iter/sec",
            "range": "stddev: 0.04577029062777959",
            "extra": "mean: 1.4695240285999944 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1e9f20d91cb3a5b5e2a64e75ddeb571bb1122434",
          "message": "Allow a single parquet file to be written on flow termination. (#35)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-02T14:26:51+02:00",
          "tree_id": "850e3fef76330fe49f12859446ec3edb12bce310",
          "url": "https://github.com/mlrun/storey/commit/1e9f20d91cb3a5b5e2a64e75ddeb571bb1122434"
        },
        "date": 1604320255043,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1358.7021794381174,
            "unit": "iter/sec",
            "range": "stddev: 0.00015912754219941377",
            "extra": "mean: 735.9964642240757 usec\nrounds: 573"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 826.9370108928148,
            "unit": "iter/sec",
            "range": "stddev: 0.00020265935897299675",
            "extra": "mean: 1.2092819487186035 msec\nrounds: 819"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.1898726778971174,
            "unit": "iter/sec",
            "range": "stddev: 0.008185624844697521",
            "extra": "mean: 313.4921362000057 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6288774988443768,
            "unit": "iter/sec",
            "range": "stddev: 0.06837032937514279",
            "extra": "mean: 1.5901348066000083 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4193.342071181542,
            "unit": "iter/sec",
            "range": "stddev: 0.00012642362162291194",
            "extra": "mean: 238.47327096742998 usec\nrounds: 2945"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2561.1719603494785,
            "unit": "iter/sec",
            "range": "stddev: 0.00010947052318185496",
            "extra": "mean: 390.44625487136267 usec\nrounds: 2566"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 12.3380554181239,
            "unit": "iter/sec",
            "range": "stddev: 0.0036527468931747183",
            "extra": "mean: 81.05004930769374 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.21241057170426,
            "unit": "iter/sec",
            "range": "stddev: 0.04248596453583806",
            "extra": "mean: 451.99567060000163 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1077.7310000331254,
            "unit": "iter/sec",
            "range": "stddev: 0.00041272613551920586",
            "extra": "mean: 927.8753232200464 usec\nrounds: 857"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 693.752598888891,
            "unit": "iter/sec",
            "range": "stddev: 0.0006147610221943053",
            "extra": "mean: 1.4414360416113647 msec\nrounds: 745"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.5391766273601006,
            "unit": "iter/sec",
            "range": "stddev: 0.03867273943920519",
            "extra": "mean: 393.82845180001027 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5499101320694799,
            "unit": "iter/sec",
            "range": "stddev: 0.08668276159441968",
            "extra": "mean: 1.8184789508000052 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1145.5502493035915,
            "unit": "iter/sec",
            "range": "stddev: 0.00023019201304180168",
            "extra": "mean: 872.942937778526 usec\nrounds: 900"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 421.24632757584885,
            "unit": "iter/sec",
            "range": "stddev: 0.0009294719705372308",
            "extra": "mean: 2.3739079358975346 msec\nrounds: 312"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7642656104600761,
            "unit": "iter/sec",
            "range": "stddev: 0.04635676991169138",
            "extra": "mean: 1.3084456323999916 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15933145179019503,
            "unit": "iter/sec",
            "range": "stddev: 0.27151008685945244",
            "extra": "mean: 6.276224742599993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1233.8709119034756,
            "unit": "iter/sec",
            "range": "stddev: 0.0002902414704583571",
            "extra": "mean: 810.4575530168822 usec\nrounds: 1094"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 782.5919392206271,
            "unit": "iter/sec",
            "range": "stddev: 0.00021073003402242985",
            "extra": "mean: 1.27780513685828 msec\nrounds: 643"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.283412754841263,
            "unit": "iter/sec",
            "range": "stddev: 0.002483669508114523",
            "extra": "mean: 304.56116080000584 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6473292859756736,
            "unit": "iter/sec",
            "range": "stddev: 0.07151918619016355",
            "extra": "mean: 1.5448088348000055 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d1e41b2bee076db00e97a0ee7cee5f491f4c4c32",
          "message": "fix load cache by key + fix persisting cache without aggregates (#36)",
          "timestamp": "2020-11-02T17:32:01+02:00",
          "tree_id": "36f1cf2760715aa3a1230e607cce27d46647549a",
          "url": "https://github.com/mlrun/storey/commit/d1e41b2bee076db00e97a0ee7cee5f491f4c4c32"
        },
        "date": 1604331654765,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1284.273055744607,
            "unit": "iter/sec",
            "range": "stddev: 0.000042459723310554146",
            "extra": "mean: 778.6506113532151 usec\nrounds: 458"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 828.3888702883997,
            "unit": "iter/sec",
            "range": "stddev: 0.00006470422583956733",
            "extra": "mean: 1.2071625245904798 msec\nrounds: 671"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.9339157635671755,
            "unit": "iter/sec",
            "range": "stddev: 0.003277133610656683",
            "extra": "mean: 254.19964739998025 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7737406014377135,
            "unit": "iter/sec",
            "range": "stddev: 0.04507154876329163",
            "extra": "mean: 1.2924228070000026 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4589.057920841476,
            "unit": "iter/sec",
            "range": "stddev: 0.00002717226484418237",
            "extra": "mean: 217.90964883194027 usec\nrounds: 2523"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3009.320284069712,
            "unit": "iter/sec",
            "range": "stddev: 0.0000934808898448619",
            "extra": "mean: 332.3009535720242 usec\nrounds: 2240"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.215659206130097,
            "unit": "iter/sec",
            "range": "stddev: 0.0015414587567653922",
            "extra": "mean: 75.6678107692236 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.6461340508890823,
            "unit": "iter/sec",
            "range": "stddev: 0.0025355662831699254",
            "extra": "mean: 377.9098038000029 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1253.5084521057638,
            "unit": "iter/sec",
            "range": "stddev: 0.00009706244883040877",
            "extra": "mean: 797.7608753416094 usec\nrounds: 730"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 789.5235377003784,
            "unit": "iter/sec",
            "range": "stddev: 0.00013586027981137108",
            "extra": "mean: 1.2665866845625273 msec\nrounds: 745"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.43001505566009,
            "unit": "iter/sec",
            "range": "stddev: 0.006041464366965065",
            "extra": "mean: 291.5439097999979 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6867196123990157,
            "unit": "iter/sec",
            "range": "stddev: 0.02162747465327752",
            "extra": "mean: 1.4561983987999951 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1181.2438891286745,
            "unit": "iter/sec",
            "range": "stddev: 0.00017794679644105656",
            "extra": "mean: 846.5652260327322 usec\nrounds: 991"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 417.334352906506,
            "unit": "iter/sec",
            "range": "stddev: 0.001112722265869441",
            "extra": "mean: 2.3961602802058968 msec\nrounds: 389"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8013219637935275,
            "unit": "iter/sec",
            "range": "stddev: 0.05805244065132203",
            "extra": "mean: 1.2479378392000058 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16330655494262297,
            "unit": "iter/sec",
            "range": "stddev: 0.014850214872514972",
            "extra": "mean: 6.123452915600024 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1292.9839240654196,
            "unit": "iter/sec",
            "range": "stddev: 0.00009804635256148504",
            "extra": "mean: 773.4048207310923 usec\nrounds: 1071"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 813.5834660322407,
            "unit": "iter/sec",
            "range": "stddev: 0.00013053092561746536",
            "extra": "mean: 1.229130189772529 msec\nrounds: 743"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.193184737343536,
            "unit": "iter/sec",
            "range": "stddev: 0.0012379048353882368",
            "extra": "mean: 238.48221880000438 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.853890940894976,
            "unit": "iter/sec",
            "range": "stddev: 0.017754049716098685",
            "extra": "mean: 1.1711097426000152 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dinaleventol@gmail.com",
            "name": "Dina Nimrodi",
            "username": "dinal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5954cd29d0b6983eda94b0c946817d86bcff33ea",
          "message": "Merge pull request #39 from dinal/frames_env_var\n\nadd framesd env var",
          "timestamp": "2020-11-03T15:21:27+02:00",
          "tree_id": "71cf8fb34cdf7939f16cad57296a9da78c77b89f",
          "url": "https://github.com/mlrun/storey/commit/5954cd29d0b6983eda94b0c946817d86bcff33ea"
        },
        "date": 1604409935709,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1182.0816102524243,
            "unit": "iter/sec",
            "range": "stddev: 0.00025771434523316365",
            "extra": "mean: 845.9652796615775 usec\nrounds: 590"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 737.5992108900402,
            "unit": "iter/sec",
            "range": "stddev: 0.00029623308890134285",
            "extra": "mean: 1.3557498235299466 msec\nrounds: 731"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.0569116229499316,
            "unit": "iter/sec",
            "range": "stddev: 0.01578142287212944",
            "extra": "mean: 327.1275467999942 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6060021557937629,
            "unit": "iter/sec",
            "range": "stddev: 0.035422858719445824",
            "extra": "mean: 1.6501591462000078 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3807.1356409661157,
            "unit": "iter/sec",
            "range": "stddev: 0.000053281831449978495",
            "extra": "mean: 262.66466296594456 usec\nrounds: 2522"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2597.4979629042105,
            "unit": "iter/sec",
            "range": "stddev: 0.00014799046369353693",
            "extra": "mean: 384.9858649675013 usec\nrounds: 2629"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.572359820864422,
            "unit": "iter/sec",
            "range": "stddev: 0.004285527091356674",
            "extra": "mean: 86.41279872727833 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.209312655556664,
            "unit": "iter/sec",
            "range": "stddev: 0.008195911042563453",
            "extra": "mean: 452.62946260000376 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1143.0011483447156,
            "unit": "iter/sec",
            "range": "stddev: 0.0003091136865499856",
            "extra": "mean: 874.8897596893856 usec\nrounds: 903"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 740.1396500449346,
            "unit": "iter/sec",
            "range": "stddev: 0.0002855289865459786",
            "extra": "mean: 1.3510963774732094 msec\nrounds: 657"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.789968634073391,
            "unit": "iter/sec",
            "range": "stddev: 0.008214113208040857",
            "extra": "mean: 358.42696859999705 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5561329636947415,
            "unit": "iter/sec",
            "range": "stddev: 0.07017264832458099",
            "extra": "mean: 1.7981311399999924 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1044.2644083170733,
            "unit": "iter/sec",
            "range": "stddev: 0.0007821545006962773",
            "extra": "mean: 957.6118768728224 usec\nrounds: 934"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 391.550603909603,
            "unit": "iter/sec",
            "range": "stddev: 0.001229363140946076",
            "extra": "mean: 2.5539483019948785 msec\nrounds: 351"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7833165341755014,
            "unit": "iter/sec",
            "range": "stddev: 0.05435840101723417",
            "extra": "mean: 1.276623122799998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1567086932501199,
            "unit": "iter/sec",
            "range": "stddev: 0.09312235575424385",
            "extra": "mean: 6.381266918 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1124.652164075349,
            "unit": "iter/sec",
            "range": "stddev: 0.0004033331422297316",
            "extra": "mean: 889.1638072133762 usec\nrounds: 804"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 649.5280910587398,
            "unit": "iter/sec",
            "range": "stddev: 0.0010342548966239837",
            "extra": "mean: 1.5395792942072544 msec\nrounds: 656"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.2595012131449876,
            "unit": "iter/sec",
            "range": "stddev: 0.011539677475354101",
            "extra": "mean: 306.795406599997 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6456687520536768,
            "unit": "iter/sec",
            "range": "stddev: 0.04168353008484943",
            "extra": "mean: 1.548781781399987 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dinaleventol@gmail.com",
            "name": "Dina Nimrodi",
            "username": "dinal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "669825ed9224e5a4343598ee70f8ff83c51e899c",
          "message": "WriteToTSDB (#38)\n\n* Add TsdbWriter\r\n\r\n* update env var name\r\n\r\n* remove framesClient wrapper\r\n\r\n* lint\r\n\r\n* rename aggr_granularity\r\n\r\nCo-authored-by: Dina Nimrodi <dinan@iguazio.com>",
          "timestamp": "2020-11-03T17:39:30+02:00",
          "tree_id": "306b12bb64cb3962f45535c5688858a6bd565e3e",
          "url": "https://github.com/mlrun/storey/commit/669825ed9224e5a4343598ee70f8ff83c51e899c"
        },
        "date": 1604418237960,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1291.153789422024,
            "unit": "iter/sec",
            "range": "stddev: 0.00011587112815481585",
            "extra": "mean: 774.5010766282483 usec\nrounds: 522"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 836.965063404317,
            "unit": "iter/sec",
            "range": "stddev: 0.0001196598035335089",
            "extra": "mean: 1.194793001194752 msec\nrounds: 837"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.674988854346818,
            "unit": "iter/sec",
            "range": "stddev: 0.010436354103380856",
            "extra": "mean: 272.1096687999989 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7215309494828279,
            "unit": "iter/sec",
            "range": "stddev: 0.04492461028718598",
            "extra": "mean: 1.3859419345999924 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4506.117228328027,
            "unit": "iter/sec",
            "range": "stddev: 0.00006516856471644419",
            "extra": "mean: 221.9205469652296 usec\nrounds: 2587"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3054.761381869875,
            "unit": "iter/sec",
            "range": "stddev: 0.00010060827290282845",
            "extra": "mean: 327.35781129584717 usec\nrounds: 2284"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.940848263329569,
            "unit": "iter/sec",
            "range": "stddev: 0.0026284410143380026",
            "extra": "mean: 71.73164653333401 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.8649072715319948,
            "unit": "iter/sec",
            "range": "stddev: 0.007728480561859589",
            "extra": "mean: 349.0514370000028 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1301.7207180525654,
            "unit": "iter/sec",
            "range": "stddev: 0.0002265953157633917",
            "extra": "mean: 768.2139387748598 usec\nrounds: 833"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 790.1945378552759,
            "unit": "iter/sec",
            "range": "stddev: 0.00018036279702792848",
            "extra": "mean: 1.265511152119796 msec\nrounds: 802"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.317160578851426,
            "unit": "iter/sec",
            "range": "stddev: 0.010466797212117128",
            "extra": "mean: 301.46264440000436 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6904236012162467,
            "unit": "iter/sec",
            "range": "stddev: 0.007822288479915203",
            "extra": "mean: 1.4483861765999961 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1207.9131579471434,
            "unit": "iter/sec",
            "range": "stddev: 0.00031971081453178356",
            "extra": "mean: 827.8740846730296 usec\nrounds: 933"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 416.4018746343772,
            "unit": "iter/sec",
            "range": "stddev: 0.0013908302642843644",
            "extra": "mean: 2.4015261719896257 msec\nrounds: 407"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8259844315920238,
            "unit": "iter/sec",
            "range": "stddev: 0.034407714615745925",
            "extra": "mean: 1.2106765717999963 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16335520595701927,
            "unit": "iter/sec",
            "range": "stddev: 0.049669752575189755",
            "extra": "mean: 6.121629207599983 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1298.44856029704,
            "unit": "iter/sec",
            "range": "stddev: 0.00012073740836642552",
            "extra": "mean: 770.1498777673831 usec\nrounds: 1039"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 815.7349872508817,
            "unit": "iter/sec",
            "range": "stddev: 0.00023397533245682025",
            "extra": "mean: 1.2258883284755409 msec\nrounds: 755"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.162798129798882,
            "unit": "iter/sec",
            "range": "stddev: 0.00658648597824703",
            "extra": "mean: 240.22303479998754 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.818973076673525,
            "unit": "iter/sec",
            "range": "stddev: 0.016671116953582792",
            "extra": "mean: 1.2210413607999953 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f093891484be23008387e4f9ce3837a4a24b93e6",
          "message": "Fuse ToDataFrame and Batch into parquet writer. (#42)\n\n* Fuse ToDataFrame into WriteToParquet.\r\n\r\n* Fuse batching functionality into parquet writer.\r\n\r\n* Replace function parameters with multiple inheritance.\r\n\r\n* Refactoring.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-05T11:50:42+02:00",
          "tree_id": "c080340ae1641883622f521fe4394dafab12bd45",
          "url": "https://github.com/mlrun/storey/commit/f093891484be23008387e4f9ce3837a4a24b93e6"
        },
        "date": 1604570098348,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1156.4360658186774,
            "unit": "iter/sec",
            "range": "stddev: 0.0001465921548945403",
            "extra": "mean: 864.7257116562415 usec\nrounds: 489"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 704.1724178943765,
            "unit": "iter/sec",
            "range": "stddev: 0.0007780488809397501",
            "extra": "mean: 1.4201067445814055 msec\nrounds: 646"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.8908336684594906,
            "unit": "iter/sec",
            "range": "stddev: 0.021525431056375395",
            "extra": "mean: 345.9209745999999 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.57432236806204,
            "unit": "iter/sec",
            "range": "stddev: 0.03565468252386451",
            "extra": "mean: 1.7411824014000046 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4587.02802382206,
            "unit": "iter/sec",
            "range": "stddev: 0.00005274011993384902",
            "extra": "mean: 218.0060803654667 usec\nrounds: 2190"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3038.3629696103803,
            "unit": "iter/sec",
            "range": "stddev: 0.00012988451564252586",
            "extra": "mean: 329.12460097821474 usec\nrounds: 2045"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.406368791829154,
            "unit": "iter/sec",
            "range": "stddev: 0.001936490123454802",
            "extra": "mean: 74.5914136428557 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.725745597575103,
            "unit": "iter/sec",
            "range": "stddev: 0.003528406416369566",
            "extra": "mean: 366.87209579999944 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1078.7102156146436,
            "unit": "iter/sec",
            "range": "stddev: 0.00047953725085263195",
            "extra": "mean: 927.0330303029579 usec\nrounds: 693"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 673.1362950561248,
            "unit": "iter/sec",
            "range": "stddev: 0.0003408777126820379",
            "extra": "mean: 1.485583242717022 msec\nrounds: 103"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.65484807735681,
            "unit": "iter/sec",
            "range": "stddev: 0.03436925505094966",
            "extra": "mean: 376.66938780000123 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5286488684017538,
            "unit": "iter/sec",
            "range": "stddev: 0.08779427082395029",
            "extra": "mean: 1.8916147555999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1079.0668166484604,
            "unit": "iter/sec",
            "range": "stddev: 0.0002489920408859704",
            "extra": "mean: 926.7266721313524 usec\nrounds: 854"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 330.3067834070952,
            "unit": "iter/sec",
            "range": "stddev: 0.0025768124760714235",
            "extra": "mean: 3.0274885356124335 msec\nrounds: 351"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7478108348564366,
            "unit": "iter/sec",
            "range": "stddev: 0.027621521062926827",
            "extra": "mean: 1.3372365756000022 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14488554329054867,
            "unit": "iter/sec",
            "range": "stddev: 0.050150504120389704",
            "extra": "mean: 6.901999863399988 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1166.4843535764546,
            "unit": "iter/sec",
            "range": "stddev: 0.00013046497100113895",
            "extra": "mean: 857.2768223885631 usec\nrounds: 670"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 637.4688496765017,
            "unit": "iter/sec",
            "range": "stddev: 0.000571948644366739",
            "extra": "mean: 1.5687041029651458 msec\nrounds: 641"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 2.9285473461644456,
            "unit": "iter/sec",
            "range": "stddev: 0.008843334398520364",
            "extra": "mean: 341.4662225999905 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.5953741039159504,
            "unit": "iter/sec",
            "range": "stddev: 0.0418184163289018",
            "extra": "mean: 1.6796162168000024 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e492f84c153b942634ef8376f573a2610e5554b5",
          "message": "rename Cache->Table (#41)\n\n* rename Cache->Table\r\n\r\n* rename cache->table variables in test",
          "timestamp": "2020-11-05T11:51:08+02:00",
          "tree_id": "fee1a41149cf8903b3252a7d3eacb8a2c70f84a1",
          "url": "https://github.com/mlrun/storey/commit/e492f84c153b942634ef8376f573a2610e5554b5"
        },
        "date": 1604570118533,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1129.9724642678725,
            "unit": "iter/sec",
            "range": "stddev: 0.00045426498915494124",
            "extra": "mean: 884.9773172551743 usec\nrounds: 539"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 679.9964165202588,
            "unit": "iter/sec",
            "range": "stddev: 0.000827672395331368",
            "extra": "mean: 1.4705959850748824 msec\nrounds: 737"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.7436967730199275,
            "unit": "iter/sec",
            "range": "stddev: 0.010842234695368332",
            "extra": "mean: 364.4717630000059 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5284761657159945,
            "unit": "iter/sec",
            "range": "stddev: 0.03976863732050526",
            "extra": "mean: 1.8922329234000017 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3427.330314622437,
            "unit": "iter/sec",
            "range": "stddev: 0.00010297484269387332",
            "extra": "mean: 291.77228577402593 usec\nrounds: 2397"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2349.16459349877,
            "unit": "iter/sec",
            "range": "stddev: 0.00022791091283834352",
            "extra": "mean: 425.6832419352244 usec\nrounds: 2046"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.391755369131355,
            "unit": "iter/sec",
            "range": "stddev: 0.0032431325694437783",
            "extra": "mean: 87.78278391666798 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.311462568070032,
            "unit": "iter/sec",
            "range": "stddev: 0.0054600072505941",
            "extra": "mean: 432.62651699999424 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1105.8939017639666,
            "unit": "iter/sec",
            "range": "stddev: 0.0002709547487307384",
            "extra": "mean: 904.2458760329001 usec\nrounds: 847"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 685.0216447246453,
            "unit": "iter/sec",
            "range": "stddev: 0.0005819386673925871",
            "extra": "mean: 1.4598078873872151 msec\nrounds: 666"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.682573928031475,
            "unit": "iter/sec",
            "range": "stddev: 0.014100312802357498",
            "extra": "mean: 372.7763062000008 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.560390863400944,
            "unit": "iter/sec",
            "range": "stddev: 0.04763007570462719",
            "extra": "mean: 1.7844687794000094 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1060.1226401367912,
            "unit": "iter/sec",
            "range": "stddev: 0.00048327051100024906",
            "extra": "mean: 943.2870897568669 usec\nrounds: 1025"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 400.6964032253064,
            "unit": "iter/sec",
            "range": "stddev: 0.001355422482333998",
            "extra": "mean: 2.495655044444492 msec\nrounds: 360"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.794377719118584,
            "unit": "iter/sec",
            "range": "stddev: 0.022494133395046377",
            "extra": "mean: 1.2588469892000091 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14535892519477064,
            "unit": "iter/sec",
            "range": "stddev: 0.20204309808546367",
            "extra": "mean: 6.879522524400005 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1125.7637086922973,
            "unit": "iter/sec",
            "range": "stddev: 0.00039971791980167456",
            "extra": "mean: 888.2858740948524 usec\nrounds: 691"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 683.6195779738362,
            "unit": "iter/sec",
            "range": "stddev: 0.000703675878705119",
            "extra": "mean: 1.462801874346367 msec\nrounds: 764"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 2.776453241451833,
            "unit": "iter/sec",
            "range": "stddev: 0.01640011535320224",
            "extra": "mean: 360.1717418000135 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.5399369019412822,
            "unit": "iter/sec",
            "range": "stddev: 0.08882243233248846",
            "extra": "mean: 1.8520682627999918 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b02d216c52e5cbab0a1ea50af13ad3b81866931e",
          "message": "remove workaround after IG-16915 fixed (#40)",
          "timestamp": "2020-11-05T11:57:32+02:00",
          "tree_id": "5b9d7c223e30993c4fbbb31ac20dd6186c89f0bf",
          "url": "https://github.com/mlrun/storey/commit/b02d216c52e5cbab0a1ea50af13ad3b81866931e"
        },
        "date": 1604570511186,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1080.3507515003628,
            "unit": "iter/sec",
            "range": "stddev: 0.0002877777476338817",
            "extra": "mean: 925.6253106791718 usec\nrounds: 618"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 674.9780821478051,
            "unit": "iter/sec",
            "range": "stddev: 0.00039193244558798855",
            "extra": "mean: 1.4815295880689388 msec\nrounds: 704"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.70368627161639,
            "unit": "iter/sec",
            "range": "stddev: 0.008940429309639451",
            "extra": "mean: 369.8653983999975 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.52104983246963,
            "unit": "iter/sec",
            "range": "stddev: 0.02374801202130888",
            "extra": "mean: 1.919202229199999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3197.2396800342826,
            "unit": "iter/sec",
            "range": "stddev: 0.0002168006924112123",
            "extra": "mean: 312.7697952220077 usec\nrounds: 2051"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2340.9887878604236,
            "unit": "iter/sec",
            "range": "stddev: 0.0002463302640964761",
            "extra": "mean: 427.16992289141325 usec\nrounds: 2075"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.37003138179839,
            "unit": "iter/sec",
            "range": "stddev: 0.0028971452139394767",
            "extra": "mean: 87.95050483333237 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.223265901297627,
            "unit": "iter/sec",
            "range": "stddev: 0.006478136153346139",
            "extra": "mean: 449.7887542000001 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1042.1714698845137,
            "unit": "iter/sec",
            "range": "stddev: 0.00046607695560313357",
            "extra": "mean: 959.5349986992188 usec\nrounds: 769"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 662.6554070560243,
            "unit": "iter/sec",
            "range": "stddev: 0.00043430503572908404",
            "extra": "mean: 1.5090799672829875 msec\nrounds: 703"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.500676149071631,
            "unit": "iter/sec",
            "range": "stddev: 0.017396276041672893",
            "extra": "mean: 399.891845399992 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5012721977590364,
            "unit": "iter/sec",
            "range": "stddev: 0.034515175868315755",
            "extra": "mean: 1.9949241239999993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1066.9255720136762,
            "unit": "iter/sec",
            "range": "stddev: 0.00026448956984370154",
            "extra": "mean: 937.2725016916004 usec\nrounds: 887"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 363.7478341073513,
            "unit": "iter/sec",
            "range": "stddev: 0.0012623900232918272",
            "extra": "mean: 2.749157262898985 msec\nrounds: 407"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7530672347516705,
            "unit": "iter/sec",
            "range": "stddev: 0.03497295335402334",
            "extra": "mean: 1.3279026810000005 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14765762602757732,
            "unit": "iter/sec",
            "range": "stddev: 0.08390378993021094",
            "extra": "mean: 6.772423659399988 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1099.5531859161854,
            "unit": "iter/sec",
            "range": "stddev: 0.00033254796739682734",
            "extra": "mean: 909.4603269843337 usec\nrounds: 945"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 672.1382172000147,
            "unit": "iter/sec",
            "range": "stddev: 0.00035552690486569084",
            "extra": "mean: 1.487789229075216 msec\nrounds: 681"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 2.813769106414339,
            "unit": "iter/sec",
            "range": "stddev: 0.019144902373878527",
            "extra": "mean: 355.39518780001345 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.5768798495187487,
            "unit": "iter/sec",
            "range": "stddev: 0.04222514578328501",
            "extra": "mean: 1.7334632174000035 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "375d20a03a4cb22a1e58b7910e1366770dbc10b6",
          "message": "Parallel persist (#37)\n\n* make persistance parallel across different keys\r\n\r\n* rename closable\r\n\r\n* move closable logic to Flow\r\n\r\n* review comments",
          "timestamp": "2020-11-05T17:08:17+02:00",
          "tree_id": "108d834d6611d62ee731ec08eb0c11d377fb4bfa",
          "url": "https://github.com/mlrun/storey/commit/375d20a03a4cb22a1e58b7910e1366770dbc10b6"
        },
        "date": 1604589104898,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1291.3994745715825,
            "unit": "iter/sec",
            "range": "stddev: 0.00012891157054405438",
            "extra": "mean: 774.3537299577629 usec\nrounds: 474"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 860.2419086604757,
            "unit": "iter/sec",
            "range": "stddev: 0.00013739443389117315",
            "extra": "mean: 1.162463709257258 msec\nrounds: 767"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.133655264720143,
            "unit": "iter/sec",
            "range": "stddev: 0.005260711142010939",
            "extra": "mean: 241.91664180000316 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8126740210270827,
            "unit": "iter/sec",
            "range": "stddev: 0.034076536500826426",
            "extra": "mean: 1.2305056813999897 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5028.284184381289,
            "unit": "iter/sec",
            "range": "stddev: 0.000015957117151845252",
            "extra": "mean: 198.8749965855492 usec\nrounds: 2343"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3577.8322498937478,
            "unit": "iter/sec",
            "range": "stddev: 0.00002446752199744934",
            "extra": "mean: 279.4988501849681 usec\nrounds: 1889"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 15.895448357905202,
            "unit": "iter/sec",
            "range": "stddev: 0.0013983921079269287",
            "extra": "mean: 62.91109111764533 msec\nrounds: 17"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.196643892166617,
            "unit": "iter/sec",
            "range": "stddev: 0.004978321671794989",
            "extra": "mean: 312.8280889999985 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1351.5222235979745,
            "unit": "iter/sec",
            "range": "stddev: 0.00011828436147318261",
            "extra": "mean: 739.90644218771 usec\nrounds: 640"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 839.7527933096928,
            "unit": "iter/sec",
            "range": "stddev: 0.00008200225207002802",
            "extra": "mean: 1.190826643230003 msec\nrounds: 768"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.7782330419294525,
            "unit": "iter/sec",
            "range": "stddev: 0.003977091844376071",
            "extra": "mean: 264.6739861999947 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7830477364095014,
            "unit": "iter/sec",
            "range": "stddev: 0.009526419051053997",
            "extra": "mean: 1.2770613507999997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1292.2083798537208,
            "unit": "iter/sec",
            "range": "stddev: 0.000056037677828418854",
            "extra": "mean: 773.8689948080981 usec\nrounds: 963"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 443.7921655623856,
            "unit": "iter/sec",
            "range": "stddev: 0.001391804239948425",
            "extra": "mean: 2.253307015306979 msec\nrounds: 392"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9613934486483373,
            "unit": "iter/sec",
            "range": "stddev: 0.015062385752108616",
            "extra": "mean: 1.040156869599997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1874617973618373,
            "unit": "iter/sec",
            "range": "stddev: 0.052319861975714546",
            "extra": "mean: 5.334420207599993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1347.1362381896859,
            "unit": "iter/sec",
            "range": "stddev: 0.00005714235512371389",
            "extra": "mean: 742.315418182072 usec\nrounds: 990"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 863.70721934363,
            "unit": "iter/sec",
            "range": "stddev: 0.000172049527961684",
            "extra": "mean: 1.157799746955855 msec\nrounds: 739"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.5279697204013445,
            "unit": "iter/sec",
            "range": "stddev: 0.006646781338395242",
            "extra": "mean: 220.84953340000766 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9166416530436912,
            "unit": "iter/sec",
            "range": "stddev: 0.02494031120117087",
            "extra": "mean: 1.09093886 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f9c290c5555a2293f4d03da9f7616acf8fd72415",
          "message": "Add docstring for WriteToParquet. (#43)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-05T17:15:38+02:00",
          "tree_id": "4e6d368ddf1f4cad946250376d62cc92cd6c6e1c",
          "url": "https://github.com/mlrun/storey/commit/f9c290c5555a2293f4d03da9f7616acf8fd72415"
        },
        "date": 1604589598928,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1156.8588639139532,
            "unit": "iter/sec",
            "range": "stddev: 0.00014861205429278303",
            "extra": "mean: 864.4096796879275 usec\nrounds: 512"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 715.5307082318401,
            "unit": "iter/sec",
            "range": "stddev: 0.00020982515183561742",
            "extra": "mean: 1.39756405769239 msec\nrounds: 624"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.09095509432426,
            "unit": "iter/sec",
            "range": "stddev: 0.00613017799822263",
            "extra": "mean: 323.5245966000093 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6237903386017934,
            "unit": "iter/sec",
            "range": "stddev: 0.022413070363901888",
            "extra": "mean: 1.6031027384000027 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3491.6163330791223,
            "unit": "iter/sec",
            "range": "stddev: 0.0000615903439253857",
            "extra": "mean: 286.4003099441738 usec\nrounds: 2152"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2430.885798332845,
            "unit": "iter/sec",
            "range": "stddev: 0.00008012929037026518",
            "extra": "mean: 411.3726776822762 usec\nrounds: 2330"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.072601893528864,
            "unit": "iter/sec",
            "range": "stddev: 0.002020949668042908",
            "extra": "mean: 90.31300950000087 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.2848269957376446,
            "unit": "iter/sec",
            "range": "stddev: 0.007231605935977142",
            "extra": "mean: 437.6698987999987 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1097.1670648991037,
            "unit": "iter/sec",
            "range": "stddev: 0.00015642571426822285",
            "extra": "mean: 911.4382230312032 usec\nrounds: 686"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 672.8125578904836,
            "unit": "iter/sec",
            "range": "stddev: 0.00021466260831286933",
            "extra": "mean: 1.4862980606892506 msec\nrounds: 725"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.5218569225997274,
            "unit": "iter/sec",
            "range": "stddev: 0.003373327865668486",
            "extra": "mean: 396.533201800014 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.50093043038549,
            "unit": "iter/sec",
            "range": "stddev: 0.02709158130112853",
            "extra": "mean: 1.9962851912000077 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1055.1960750179746,
            "unit": "iter/sec",
            "range": "stddev: 0.0001632836887684688",
            "extra": "mean: 947.6911672391935 usec\nrounds: 873"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 365.86862036380626,
            "unit": "iter/sec",
            "range": "stddev: 0.0016018965978014748",
            "extra": "mean: 2.7332215564309315 msec\nrounds: 381"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7662457371874276,
            "unit": "iter/sec",
            "range": "stddev: 0.031424449035645265",
            "extra": "mean: 1.3050643565999962 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1564275555068399,
            "unit": "iter/sec",
            "range": "stddev: 0.05964091336905502",
            "extra": "mean: 6.392735581400006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1123.4460078380675,
            "unit": "iter/sec",
            "range": "stddev: 0.00016680595391008188",
            "extra": "mean: 890.1184329493288 usec\nrounds: 1044"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 704.8603285199936,
            "unit": "iter/sec",
            "range": "stddev: 0.0002056765464488375",
            "extra": "mean: 1.4187207869957952 msec\nrounds: 446"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.2898112448164496,
            "unit": "iter/sec",
            "range": "stddev: 0.005007062163781765",
            "extra": "mean: 303.968807199999 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6784265857407823,
            "unit": "iter/sec",
            "range": "stddev: 0.020505603981427478",
            "extra": "mean: 1.473998839399974 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e765974b96417d994d7d6a08598cbe1c37ded666",
          "message": "load additional column when loading aggregates (#28)\n\n* load additional column when loading aggregates\r\n\r\n* add enriching parameters to aggregate steps\r\n\r\n* add py docs",
          "timestamp": "2020-11-05T17:17:35+02:00",
          "tree_id": "7200b98878a5cacefaa39cc709b2ce4fe6cb439c",
          "url": "https://github.com/mlrun/storey/commit/e765974b96417d994d7d6a08598cbe1c37ded666"
        },
        "date": 1604589699172,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1212.559484803119,
            "unit": "iter/sec",
            "range": "stddev: 0.00010718740358907616",
            "extra": "mean: 824.7018084744668 usec\nrounds: 590"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 707.4988455866895,
            "unit": "iter/sec",
            "range": "stddev: 0.0007354483166163188",
            "extra": "mean: 1.4134298681021247 msec\nrounds: 743"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.7447483348129973,
            "unit": "iter/sec",
            "range": "stddev: 0.013425899631289658",
            "extra": "mean: 364.3321273999902 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5811840502525026,
            "unit": "iter/sec",
            "range": "stddev: 0.031130010159838715",
            "extra": "mean: 1.720625332999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3502.164843572069,
            "unit": "iter/sec",
            "range": "stddev: 0.00006065160636576992",
            "extra": "mean: 285.5376730297023 usec\nrounds: 2462"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2357.2024595419225,
            "unit": "iter/sec",
            "range": "stddev: 0.00008760814559239799",
            "extra": "mean: 424.23169717646186 usec\nrounds: 2196"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 10.728261692482311,
            "unit": "iter/sec",
            "range": "stddev: 0.0027925115910287382",
            "extra": "mean: 93.21174563636316 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.1747837531394705,
            "unit": "iter/sec",
            "range": "stddev: 0.003753620469787618",
            "extra": "mean: 459.8158316000024 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1150.0713111034418,
            "unit": "iter/sec",
            "range": "stddev: 0.0002043553242684003",
            "extra": "mean: 869.5112992954714 usec\nrounds: 852"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 719.0021303456248,
            "unit": "iter/sec",
            "range": "stddev: 0.0003102698092860124",
            "extra": "mean: 1.3908164632548436 msec\nrounds: 762"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.615293763619999,
            "unit": "iter/sec",
            "range": "stddev: 0.017896098521536324",
            "extra": "mean: 382.36622359999615 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5417124878372787,
            "unit": "iter/sec",
            "range": "stddev: 0.007202579436407335",
            "extra": "mean: 1.8459976877999964 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1140.0905486496786,
            "unit": "iter/sec",
            "range": "stddev: 0.0002241495879300722",
            "extra": "mean: 877.1233137441568 usec\nrounds: 1004"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 391.2330639239773,
            "unit": "iter/sec",
            "range": "stddev: 0.001242766073348256",
            "extra": "mean: 2.556021185863564 msec\nrounds: 382"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.751667551537232,
            "unit": "iter/sec",
            "range": "stddev: 0.02015067890913442",
            "extra": "mean: 1.3303753739999877 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14948385217119695,
            "unit": "iter/sec",
            "range": "stddev: 0.065956680119908",
            "extra": "mean: 6.689685778599994 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1125.9593139067956,
            "unit": "iter/sec",
            "range": "stddev: 0.00033683242363582734",
            "extra": "mean: 888.1315582623065 usec\nrounds: 944"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 705.5219288322428,
            "unit": "iter/sec",
            "range": "stddev: 0.000220245348712189",
            "extra": "mean: 1.4173903873621163 msec\nrounds: 728"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.0701097929164067,
            "unit": "iter/sec",
            "range": "stddev: 0.007939818085914234",
            "extra": "mean: 325.7212502000016 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6027370071715815,
            "unit": "iter/sec",
            "range": "stddev: 0.0733921741191871",
            "extra": "mean: 1.6590983929999994 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "dd2ad0a8f80eeb8aa5b0ddc100326830edb12845",
          "message": "Readme: v3io/storey -> mlrun/storey",
          "timestamp": "2020-11-05T17:18:47+02:00",
          "tree_id": "d58b75119aeb26e6bf7805036372f417f6e4fb90",
          "url": "https://github.com/mlrun/storey/commit/dd2ad0a8f80eeb8aa5b0ddc100326830edb12845"
        },
        "date": 1604589763598,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1096.0437549466437,
            "unit": "iter/sec",
            "range": "stddev: 0.0007495876387359475",
            "extra": "mean: 912.3723350339063 usec\nrounds: 588"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 687.2232283443644,
            "unit": "iter/sec",
            "range": "stddev: 0.000681034762396933",
            "extra": "mean: 1.4551312568540022 msec\nrounds: 693"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.1568361121221273,
            "unit": "iter/sec",
            "range": "stddev: 0.007297669616794962",
            "extra": "mean: 316.77285879999886 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6281029593430829,
            "unit": "iter/sec",
            "range": "stddev: 0.010322357812941994",
            "extra": "mean: 1.592095667000001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3595.9216073841535,
            "unit": "iter/sec",
            "range": "stddev: 0.0002353389822388416",
            "extra": "mean: 278.092825479432 usec\nrounds: 2504"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2558.354543409955,
            "unit": "iter/sec",
            "range": "stddev: 0.00011003002276011243",
            "extra": "mean: 390.8762382351938 usec\nrounds: 2380"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.580782545492747,
            "unit": "iter/sec",
            "range": "stddev: 0.006024037030647921",
            "extra": "mean: 86.34995053846349 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.3718410056837076,
            "unit": "iter/sec",
            "range": "stddev: 0.02396863571642823",
            "extra": "mean: 421.61342079999145 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1211.578907752707,
            "unit": "iter/sec",
            "range": "stddev: 0.00026622021994686016",
            "extra": "mean: 825.369271123122 usec\nrounds: 793"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 741.01160767258,
            "unit": "iter/sec",
            "range": "stddev: 0.000501162755537224",
            "extra": "mean: 1.3495065254657324 msec\nrounds: 805"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.8267421594669377,
            "unit": "iter/sec",
            "range": "stddev: 0.012360269123861345",
            "extra": "mean: 353.7641368000038 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5584744219356433,
            "unit": "iter/sec",
            "range": "stddev: 0.03431539196684715",
            "extra": "mean: 1.7905923006000024 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1188.4562367557053,
            "unit": "iter/sec",
            "range": "stddev: 0.0002512634254550049",
            "extra": "mean: 841.427701814111 usec\nrounds: 882"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 387.63780795575883,
            "unit": "iter/sec",
            "range": "stddev: 0.001800446651027708",
            "extra": "mean: 2.5797277238605427 msec\nrounds: 373"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7994260553252444,
            "unit": "iter/sec",
            "range": "stddev: 0.0374714451527915",
            "extra": "mean: 1.2508974323999893 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1554775630905992,
            "unit": "iter/sec",
            "range": "stddev: 0.20370822219063658",
            "extra": "mean: 6.4317962034 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1225.9885142943137,
            "unit": "iter/sec",
            "range": "stddev: 0.00012994318421533625",
            "extra": "mean: 815.6683266935872 usec\nrounds: 1004"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 766.8369824501528,
            "unit": "iter/sec",
            "range": "stddev: 0.0001936028505038096",
            "extra": "mean: 1.3040581282411006 msec\nrounds: 733"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.4401158692670757,
            "unit": "iter/sec",
            "range": "stddev: 0.013666520591860569",
            "extra": "mean: 290.6878832000075 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6837599978033024,
            "unit": "iter/sec",
            "range": "stddev: 0.023405777532114643",
            "extra": "mean: 1.462501467200002 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dinaleventol@gmail.com",
            "name": "Dina Nimrodi",
            "username": "dinal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "aed761eab5491a00f51e09c7010c6cd6e76e48fe",
          "message": "tsdb docstring (#44)\n\n* tsdb docstring\r\n\r\n* doc updates\r\n\r\nCo-authored-by: Dina Nimrodi <dinan@iguazio.com>",
          "timestamp": "2020-11-08T12:47:08+02:00",
          "tree_id": "927e8cb39c02f437cec9ced26001a0c3272c70a2",
          "url": "https://github.com/mlrun/storey/commit/aed761eab5491a00f51e09c7010c6cd6e76e48fe"
        },
        "date": 1604832649238,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1229.7312050378564,
            "unit": "iter/sec",
            "range": "stddev: 0.0002707216204094617",
            "extra": "mean: 813.1858376068581 usec\nrounds: 585"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 846.1861766383718,
            "unit": "iter/sec",
            "range": "stddev: 0.0003425877892124032",
            "extra": "mean: 1.1817730277427618 msec\nrounds: 793"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.9158851823368037,
            "unit": "iter/sec",
            "range": "stddev: 0.00788894411555977",
            "extra": "mean: 255.37010240000203 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7801238830079533,
            "unit": "iter/sec",
            "range": "stddev: 0.04369735560372423",
            "extra": "mean: 1.2818476934000045 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4820.4175785496545,
            "unit": "iter/sec",
            "range": "stddev: 0.00001236491812083949",
            "extra": "mean: 207.45090725125013 usec\nrounds: 1779"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3232.624004330592,
            "unit": "iter/sec",
            "range": "stddev: 0.00001203220320791605",
            "extra": "mean: 309.3462149202467 usec\nrounds: 2252"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.358798024098457,
            "unit": "iter/sec",
            "range": "stddev: 0.00019689809203638048",
            "extra": "mean: 69.64371239999991 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.892482372077151,
            "unit": "iter/sec",
            "range": "stddev: 0.00042222827142540713",
            "extra": "mean: 345.7238009999969 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1262.033165999422,
            "unit": "iter/sec",
            "range": "stddev: 0.00018235264509291907",
            "extra": "mean: 792.3722029983941 usec\nrounds: 867"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 820.0035420577344,
            "unit": "iter/sec",
            "range": "stddev: 0.00006283369545437078",
            "extra": "mean: 1.2195069273610437 msec\nrounds: 826"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.571229873300199,
            "unit": "iter/sec",
            "range": "stddev: 0.006006594240960919",
            "extra": "mean: 280.01557879999837 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7292930563743782,
            "unit": "iter/sec",
            "range": "stddev: 0.012402223151820083",
            "extra": "mean: 1.3711908968000046 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1254.3814702868997,
            "unit": "iter/sec",
            "range": "stddev: 0.00010616685394978395",
            "extra": "mean: 797.2056536926378 usec\nrounds: 1002"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 409.1940968500258,
            "unit": "iter/sec",
            "range": "stddev: 0.0014041483828959658",
            "extra": "mean: 2.4438280212202357 msec\nrounds: 377"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8647555751668385,
            "unit": "iter/sec",
            "range": "stddev: 0.02949247834708135",
            "extra": "mean: 1.1563961293999967 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16956310855925194,
            "unit": "iter/sec",
            "range": "stddev: 0.0854734855122281",
            "extra": "mean: 5.8975092429999965 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1298.8837097359483,
            "unit": "iter/sec",
            "range": "stddev: 0.00014443113062815338",
            "extra": "mean: 769.8918636860041 usec\nrounds: 917"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 815.5654279298273,
            "unit": "iter/sec",
            "range": "stddev: 0.0003632044622628105",
            "extra": "mean: 1.2261431955720932 msec\nrounds: 813"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.213804765899815,
            "unit": "iter/sec",
            "range": "stddev: 0.004228098344908486",
            "extra": "mean: 237.3152188000006 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8387734785995388,
            "unit": "iter/sec",
            "range": "stddev: 0.03942208422272647",
            "extra": "mean: 1.1922169996000036 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c4a8650a68a1082638c1b7f160d6da49014e97e4",
          "message": "Simplify Batching base class. (#46)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-08T14:40:54+02:00",
          "tree_id": "6cfab2f29201fd7ec268c733e3d961c4d1d24844",
          "url": "https://github.com/mlrun/storey/commit/c4a8650a68a1082638c1b7f160d6da49014e97e4"
        },
        "date": 1604839472429,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1329.000343146213,
            "unit": "iter/sec",
            "range": "stddev: 0.00005105437906261855",
            "extra": "mean: 752.4452534245754 usec\nrounds: 438"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 854.8931864148467,
            "unit": "iter/sec",
            "range": "stddev: 0.00007811372251029586",
            "extra": "mean: 1.169736776349436 msec\nrounds: 778"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.96433895385708,
            "unit": "iter/sec",
            "range": "stddev: 0.004952857434807276",
            "extra": "mean: 252.24886459999996 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8087280112480535,
            "unit": "iter/sec",
            "range": "stddev: 0.019973123192124218",
            "extra": "mean: 1.2365096621999896 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4906.052313005017,
            "unit": "iter/sec",
            "range": "stddev: 0.000014235835632545702",
            "extra": "mean: 203.82986894558567 usec\nrounds: 2457"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3268.8462812652324,
            "unit": "iter/sec",
            "range": "stddev: 0.00005630909959117509",
            "extra": "mean: 305.9183314098643 usec\nrounds: 2079"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.997156618777582,
            "unit": "iter/sec",
            "range": "stddev: 0.002101102708746181",
            "extra": "mean: 66.67930631249952 msec\nrounds: 16"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.9764687671616126,
            "unit": "iter/sec",
            "range": "stddev: 0.005155091698037941",
            "extra": "mean: 335.9685850000062 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1334.689240340826,
            "unit": "iter/sec",
            "range": "stddev: 0.00006170114288622309",
            "extra": "mean: 749.2380771307037 usec\nrounds: 739"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 835.9363867067166,
            "unit": "iter/sec",
            "range": "stddev: 0.0000870914575281318",
            "extra": "mean: 1.196263275414573 msec\nrounds: 846"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.718670172190579,
            "unit": "iter/sec",
            "range": "stddev: 0.006262919795913204",
            "extra": "mean: 268.91333560000135 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.750966546513019,
            "unit": "iter/sec",
            "range": "stddev: 0.03537575153148389",
            "extra": "mean: 1.3316172400000028 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1318.934879203229,
            "unit": "iter/sec",
            "range": "stddev: 0.00006637361523291699",
            "extra": "mean: 758.187546457261 usec\nrounds: 635"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 446.9663340348749,
            "unit": "iter/sec",
            "range": "stddev: 0.0013179172305302245",
            "extra": "mean: 2.2373049687495574 msec\nrounds: 384"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8729317201672178,
            "unit": "iter/sec",
            "range": "stddev: 0.010226652942787897",
            "extra": "mean: 1.1455649702000072 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1750866277009569,
            "unit": "iter/sec",
            "range": "stddev: 0.03316131883223658",
            "extra": "mean: 5.71145845420001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1344.1833179930481,
            "unit": "iter/sec",
            "range": "stddev: 0.00006108074801131395",
            "extra": "mean: 743.9461467897579 usec\nrounds: 1090"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 845.9135507295185,
            "unit": "iter/sec",
            "range": "stddev: 0.00016419399628536308",
            "extra": "mean: 1.1821538963852711 msec\nrounds: 830"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.348769308548929,
            "unit": "iter/sec",
            "range": "stddev: 0.00652685817990017",
            "extra": "mean: 229.9501143999919 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8755241958602497,
            "unit": "iter/sec",
            "range": "stddev: 0.016977377073169108",
            "extra": "mean: 1.1421728888000018 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dinaleventol@gmail.com",
            "name": "Dina Nimrodi",
            "username": "dinal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "95add2c5b35b489e9b0ad3d202d9f57dc98d8917",
          "message": "tsdb batching (#47)\n\n* tsdb batching\r\n\r\n* align to dev\r\n\r\nCo-authored-by: Dina Nimrodi <dinan@iguazio.com>",
          "timestamp": "2020-11-08T15:09:35+02:00",
          "tree_id": "7839f2561b68c30d4d7e385d0404f67933ad0639",
          "url": "https://github.com/mlrun/storey/commit/95add2c5b35b489e9b0ad3d202d9f57dc98d8917"
        },
        "date": 1604841204528,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1382.3625446510168,
            "unit": "iter/sec",
            "range": "stddev: 0.000054043890650452394",
            "extra": "mean: 723.3992297241056 usec\nrounds: 444"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 881.833860489867,
            "unit": "iter/sec",
            "range": "stddev: 0.00008805105190452317",
            "extra": "mean: 1.134000456100076 msec\nrounds: 763"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.062402724243691,
            "unit": "iter/sec",
            "range": "stddev: 0.0034292329066690514",
            "extra": "mean: 246.15974039998036 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8203627715677588,
            "unit": "iter/sec",
            "range": "stddev: 0.013261912536739655",
            "extra": "mean: 1.2189729162001641 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5080.38864075491,
            "unit": "iter/sec",
            "range": "stddev: 0.00002108172943771193",
            "extra": "mean: 196.83533499346757 usec\nrounds: 2806"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3488.614522606226,
            "unit": "iter/sec",
            "range": "stddev: 0.000055644969404536466",
            "extra": "mean: 286.64674572670583 usec\nrounds: 2517"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 15.346492148230384,
            "unit": "iter/sec",
            "range": "stddev: 0.0017546241489681698",
            "extra": "mean: 65.16147080004278 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.03822780007308,
            "unit": "iter/sec",
            "range": "stddev: 0.006912831577296461",
            "extra": "mean: 329.1392435998205 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1391.4940699168062,
            "unit": "iter/sec",
            "range": "stddev: 0.00006648093105388492",
            "extra": "mean: 718.6520026346842 usec\nrounds: 759"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 855.5534118516941,
            "unit": "iter/sec",
            "range": "stddev: 0.00009115489976213618",
            "extra": "mean: 1.1688340974944822 msec\nrounds: 800"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.846585900599852,
            "unit": "iter/sec",
            "range": "stddev: 0.006971995483421185",
            "extra": "mean: 259.97079640001175 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7817341888117879,
            "unit": "iter/sec",
            "range": "stddev: 0.016066505424146164",
            "extra": "mean: 1.279207196400057 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1317.2836891991412,
            "unit": "iter/sec",
            "range": "stddev: 0.00005897197523429325",
            "extra": "mean: 759.1379200997792 usec\nrounds: 1164"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 440.2896605918384,
            "unit": "iter/sec",
            "range": "stddev: 0.0013610177012897949",
            "extra": "mean: 2.2712320763013096 msec\nrounds: 367"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9353768191426945,
            "unit": "iter/sec",
            "range": "stddev: 0.01830339746408847",
            "extra": "mean: 1.0690878579998753 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.18463413093114175,
            "unit": "iter/sec",
            "range": "stddev: 0.037327116224873516",
            "extra": "mean: 5.416116700400016 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1378.906845093846,
            "unit": "iter/sec",
            "range": "stddev: 0.000059699985165082276",
            "extra": "mean: 725.2121516098077 usec\nrounds: 1062"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 874.9841986690955,
            "unit": "iter/sec",
            "range": "stddev: 0.00008720179127205086",
            "extra": "mean: 1.1428777817028708 msec\nrounds: 962"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.3540904531926925,
            "unit": "iter/sec",
            "range": "stddev: 0.0032844701819565543",
            "extra": "mean: 229.66909180004222 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9075437252478592,
            "unit": "iter/sec",
            "range": "stddev: 0.011166847633620745",
            "extra": "mean: 1.1018752840001071 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a0a3a519e317b6feab4a2ef4c99e593744410d15",
          "message": "Add docstrings to public API. Make non-API members private. (#48)\n\n* Add docstrings to public API. Make non-API members private.\r\n\r\n* HttpRequest params.\r\n\r\n* MapWithState docstring.\r\n\r\n* HttpResponse params.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-09T12:33:46+02:00",
          "tree_id": "e77b76d35848d1fdae18fc2503e164b0f9a95791",
          "url": "https://github.com/mlrun/storey/commit/a0a3a519e317b6feab4a2ef4c99e593744410d15"
        },
        "date": 1604918237093,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1496.5555758566816,
            "unit": "iter/sec",
            "range": "stddev: 0.000022425033474692907",
            "extra": "mean: 668.2010452084712 usec\nrounds: 553"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 887.4177187562466,
            "unit": "iter/sec",
            "range": "stddev: 0.0008683014384363499",
            "extra": "mean: 1.1268650364583008 msec\nrounds: 576"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.640245346624848,
            "unit": "iter/sec",
            "range": "stddev: 0.0034039461890565783",
            "extra": "mean: 215.50584619999995 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9232966779135503,
            "unit": "iter/sec",
            "range": "stddev: 0.013230992763891706",
            "extra": "mean: 1.0830754880000029 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5381.112339612276,
            "unit": "iter/sec",
            "range": "stddev: 0.00000880292800861374",
            "extra": "mean: 185.8351836735772 usec\nrounds: 2597"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3583.629938134168,
            "unit": "iter/sec",
            "range": "stddev: 0.000045511030404992175",
            "extra": "mean: 279.04666979109294 usec\nrounds: 2347"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 15.831125630919448,
            "unit": "iter/sec",
            "range": "stddev: 0.0001314038401088516",
            "extra": "mean: 63.166702312495104 msec\nrounds: 16"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.181198093142292,
            "unit": "iter/sec",
            "range": "stddev: 0.0006940916923856214",
            "extra": "mean: 314.34697579999806 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1488.3358279690308,
            "unit": "iter/sec",
            "range": "stddev: 0.00003761496238410376",
            "extra": "mean: 671.8913710251742 usec\nrounds: 849"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 944.8676062617823,
            "unit": "iter/sec",
            "range": "stddev: 0.00010375119425386597",
            "extra": "mean: 1.0583493320893285 msec\nrounds: 804"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.064390001046263,
            "unit": "iter/sec",
            "range": "stddev: 0.00175844244163429",
            "extra": "mean: 246.0393809999971 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8127084081098519,
            "unit": "iter/sec",
            "range": "stddev: 0.0071746952489520855",
            "extra": "mean: 1.2304536166000049 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1437.3831726988549,
            "unit": "iter/sec",
            "range": "stddev: 0.00002583507971518245",
            "extra": "mean: 695.7087149715153 usec\nrounds: 1042"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 482.0944236938974,
            "unit": "iter/sec",
            "range": "stddev: 0.0012501319839234592",
            "extra": "mean: 2.0742824452060935 msec\nrounds: 438"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.0356660625946246,
            "unit": "iter/sec",
            "range": "stddev: 0.010295969725030281",
            "extra": "mean: 965.5621981999957 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.20631623504534483,
            "unit": "iter/sec",
            "range": "stddev: 0.024099923053764377",
            "extra": "mean: 4.846928307799999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1499.4190586417817,
            "unit": "iter/sec",
            "range": "stddev: 0.00003254804726082133",
            "extra": "mean: 666.9249628625034 usec\nrounds: 1104"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 938.8574453603103,
            "unit": "iter/sec",
            "range": "stddev: 0.00009746395028300781",
            "extra": "mean: 1.065124428572034 msec\nrounds: 553"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.01454526102352,
            "unit": "iter/sec",
            "range": "stddev: 0.007827249503267629",
            "extra": "mean: 199.4198771666665 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.98990555393273,
            "unit": "iter/sec",
            "range": "stddev: 0.012510229821102",
            "extra": "mean: 1.0101973829999906 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e4dba6364ada5fcc7c4daff8ada4e998feb84b10",
          "message": "JoinWithTable (#45)\n\n* joinWithTable + tests\r\n\r\n* add closeable\r\n\r\n* change default attributes to fetch\r\n\r\n* review comments",
          "timestamp": "2020-11-09T16:34:52+02:00",
          "tree_id": "ba2a1926110959607f977afcf46a8b3951a29b2e",
          "url": "https://github.com/mlrun/storey/commit/e4dba6364ada5fcc7c4daff8ada4e998feb84b10"
        },
        "date": 1604932735423,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1059.8322921372287,
            "unit": "iter/sec",
            "range": "stddev: 0.0007455254313492875",
            "extra": "mean: 943.5455094347308 usec\nrounds: 636"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 744.5306509465119,
            "unit": "iter/sec",
            "range": "stddev: 0.0004063036993685087",
            "extra": "mean: 1.3431280481585457 msec\nrounds: 706"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.1169124071162644,
            "unit": "iter/sec",
            "range": "stddev: 0.01359075417902283",
            "extra": "mean: 320.8303183999931 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6569695653298783,
            "unit": "iter/sec",
            "range": "stddev: 0.02753238297980181",
            "extra": "mean: 1.5221405263999999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4767.867958020856,
            "unit": "iter/sec",
            "range": "stddev: 0.000012496622794116932",
            "extra": "mean: 209.73735195785505 usec\nrounds: 1915"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3189.6557838663052,
            "unit": "iter/sec",
            "range": "stddev: 0.000011951174673719578",
            "extra": "mean: 313.51345341341545 usec\nrounds: 2168"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.13314103844453,
            "unit": "iter/sec",
            "range": "stddev: 0.0001249987905212755",
            "extra": "mean: 70.75567966666654 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.8404484056833437,
            "unit": "iter/sec",
            "range": "stddev: 0.000899143815137454",
            "extra": "mean: 352.0570899999939 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1192.047611971992,
            "unit": "iter/sec",
            "range": "stddev: 0.00025229430956625053",
            "extra": "mean: 838.8926666659819 usec\nrounds: 855"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 670.4521422703805,
            "unit": "iter/sec",
            "range": "stddev: 0.000643428852937693",
            "extra": "mean: 1.4915307699870384 msec\nrounds: 813"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.7701095037085035,
            "unit": "iter/sec",
            "range": "stddev: 0.007201433333238318",
            "extra": "mean: 360.99655939999593 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6139154273631118,
            "unit": "iter/sec",
            "range": "stddev: 0.14819521465454086",
            "extra": "mean: 1.6288888590000057 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1139.9477972755467,
            "unit": "iter/sec",
            "range": "stddev: 0.00018043137286521626",
            "extra": "mean: 877.2331525969705 usec\nrounds: 924"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 357.43012321347794,
            "unit": "iter/sec",
            "range": "stddev: 0.0019239436522956955",
            "extra": "mean: 2.7977496440688694 msec\nrounds: 354"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7262265547695218,
            "unit": "iter/sec",
            "range": "stddev: 0.04076648971913635",
            "extra": "mean: 1.3769807691999971 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1459347815287504,
            "unit": "iter/sec",
            "range": "stddev: 0.10724374890552185",
            "extra": "mean: 6.852376037599998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1193.3828719994285,
            "unit": "iter/sec",
            "range": "stddev: 0.0004755332433026094",
            "extra": "mean: 837.9540409563367 usec\nrounds: 879"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 750.5637451258575,
            "unit": "iter/sec",
            "range": "stddev: 0.0003403733184437713",
            "extra": "mean: 1.3323318725344455 msec\nrounds: 659"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.3485318932169337,
            "unit": "iter/sec",
            "range": "stddev: 0.02450429965773465",
            "extra": "mean: 298.63833819999854 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.652377557094382,
            "unit": "iter/sec",
            "range": "stddev: 0.04677779744096151",
            "extra": "mean: 1.5328546930000015 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7e9b8ed95ba94a2c8a6999b2f8d3acad459c9106",
          "message": "better handle of flow closeables (#49)\n\n* better handle of flow closeables\r\n\r\n* move closable to after the loop",
          "timestamp": "2020-11-10T10:37:03+02:00",
          "tree_id": "4c6ffe27eb64464b459e348b8dd94a5fcddc21a8",
          "url": "https://github.com/mlrun/storey/commit/7e9b8ed95ba94a2c8a6999b2f8d3acad459c9106"
        },
        "date": 1604997632788,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1397.8077901836016,
            "unit": "iter/sec",
            "range": "stddev: 0.0000649564055759271",
            "extra": "mean: 715.4059428075232 usec\nrounds: 577"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 893.0929093820976,
            "unit": "iter/sec",
            "range": "stddev: 0.0001219341414085527",
            "extra": "mean: 1.1197043325446039 msec\nrounds: 845"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.1704312002241695,
            "unit": "iter/sec",
            "range": "stddev: 0.007803690982331172",
            "extra": "mean: 239.7833586000047 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8277630769170682,
            "unit": "iter/sec",
            "range": "stddev: 0.0372950050110601",
            "extra": "mean: 1.2080751460000045 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5377.561106745098,
            "unit": "iter/sec",
            "range": "stddev: 0.000019025575177332942",
            "extra": "mean: 185.95790547980863 usec\nrounds: 1989"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3567.920534770399,
            "unit": "iter/sec",
            "range": "stddev: 0.000025275601864817863",
            "extra": "mean: 280.2753004879778 usec\nrounds: 2253"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 16.379718013591628,
            "unit": "iter/sec",
            "range": "stddev: 0.002848181762411184",
            "extra": "mean: 61.05111206250413 msec\nrounds: 16"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.2595898693671277,
            "unit": "iter/sec",
            "range": "stddev: 0.014798012768101554",
            "extra": "mean: 306.787062199993 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1398.7977500919708,
            "unit": "iter/sec",
            "range": "stddev: 0.00018843303927162944",
            "extra": "mean: 714.899634299705 usec\nrounds: 793"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 869.4249797876395,
            "unit": "iter/sec",
            "range": "stddev: 0.00014017713781299125",
            "extra": "mean: 1.1501854941460894 msec\nrounds: 854"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.944864067700388,
            "unit": "iter/sec",
            "range": "stddev: 0.007777908127254322",
            "extra": "mean: 253.49415919999956 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8058207790956469,
            "unit": "iter/sec",
            "range": "stddev: 0.007543042028685115",
            "extra": "mean: 1.2409707293999985 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1362.1383429141829,
            "unit": "iter/sec",
            "range": "stddev: 0.0000786826953092187",
            "extra": "mean: 734.1398215548226 usec\nrounds: 1132"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 468.2151217163059,
            "unit": "iter/sec",
            "range": "stddev: 0.001112038435900647",
            "extra": "mean: 2.1357704047113315 msec\nrounds: 467"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9699523755342794,
            "unit": "iter/sec",
            "range": "stddev: 0.029612993880938458",
            "extra": "mean: 1.030978453400013 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1908838623626954,
            "unit": "iter/sec",
            "range": "stddev: 0.1272465500953861",
            "extra": "mean: 5.238787541399995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1356.5711512872556,
            "unit": "iter/sec",
            "range": "stddev: 0.00005636549581269127",
            "extra": "mean: 737.1526359314779 usec\nrounds: 1052"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 869.5041010754968,
            "unit": "iter/sec",
            "range": "stddev: 0.00007785852157497184",
            "extra": "mean: 1.1500808320088334 msec\nrounds: 756"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.36849970411212,
            "unit": "iter/sec",
            "range": "stddev: 0.005157630220825979",
            "extra": "mean: 228.91154119998873 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8938275353399312,
            "unit": "iter/sec",
            "range": "stddev: 0.022618976635477007",
            "extra": "mean: 1.118784061200006 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e1f1cb61b4c87272623aee68410383babaf43e90",
          "message": "Improve default termination result semantics. (#50)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-11T11:22:00+02:00",
          "tree_id": "68d7c04d7bafcf95a5a7fe2d5ad071a45d6dd02e",
          "url": "https://github.com/mlrun/storey/commit/e1f1cb61b4c87272623aee68410383babaf43e90"
        },
        "date": 1605086771737,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1056.2568380342013,
            "unit": "iter/sec",
            "range": "stddev: 0.00041027659870520173",
            "extra": "mean: 946.7394330540848 usec\nrounds: 478"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 672.0741797040249,
            "unit": "iter/sec",
            "range": "stddev: 0.00045005130372033754",
            "extra": "mean: 1.4879309906540235 msec\nrounds: 749"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.9059830753743743,
            "unit": "iter/sec",
            "range": "stddev: 0.01440349203907205",
            "extra": "mean: 344.1176270000028 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5839148903266707,
            "unit": "iter/sec",
            "range": "stddev: 0.03934606757470019",
            "extra": "mean: 1.7125783510000077 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3498.1278109308887,
            "unit": "iter/sec",
            "range": "stddev: 0.00007238898940872368",
            "extra": "mean: 285.8671992701975 usec\nrounds: 2193"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2358.799070202942,
            "unit": "iter/sec",
            "range": "stddev: 0.00021919702804411932",
            "extra": "mean: 423.94454560894997 usec\nrounds: 2357"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.728041415868196,
            "unit": "iter/sec",
            "range": "stddev: 0.0034110665948160154",
            "extra": "mean: 85.2657289090902 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.2706555235312833,
            "unit": "iter/sec",
            "range": "stddev: 0.008868915986240285",
            "extra": "mean: 440.4014565999944 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1064.1704066719556,
            "unit": "iter/sec",
            "range": "stddev: 0.0005070519300124602",
            "extra": "mean: 939.6991249995012 usec\nrounds: 712"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 639.8374730588721,
            "unit": "iter/sec",
            "range": "stddev: 0.0005280729181293244",
            "extra": "mean: 1.562896895080712 msec\nrounds: 610"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.524346836779642,
            "unit": "iter/sec",
            "range": "stddev: 0.007877102317819164",
            "extra": "mean: 396.14207739999756 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.4949344375451019,
            "unit": "iter/sec",
            "range": "stddev: 0.03046288209566558",
            "extra": "mean: 2.020469630200006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 995.7501613069096,
            "unit": "iter/sec",
            "range": "stddev: 0.0007036001788762708",
            "extra": "mean: 1.0042679769064888 msec\nrounds: 866"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 371.5438569293143,
            "unit": "iter/sec",
            "range": "stddev: 0.0014325624642899095",
            "extra": "mean: 2.6914723022597267 msec\nrounds: 354"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7552864388335969,
            "unit": "iter/sec",
            "range": "stddev: 0.029195510056884293",
            "extra": "mean: 1.3240009995999913 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14819944001385674,
            "unit": "iter/sec",
            "range": "stddev: 0.02525552147493717",
            "extra": "mean: 6.747663823200003 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1029.730232223818,
            "unit": "iter/sec",
            "range": "stddev: 0.0004485640918892553",
            "extra": "mean: 971.1281350265766 usec\nrounds: 748"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 651.2787386380212,
            "unit": "iter/sec",
            "range": "stddev: 0.000540457011074442",
            "extra": "mean: 1.5354408806454176 msec\nrounds: 620"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.160129694326556,
            "unit": "iter/sec",
            "range": "stddev: 0.010209700802034278",
            "extra": "mean: 316.4427086000046 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6276022489354925,
            "unit": "iter/sec",
            "range": "stddev: 0.04958857968932845",
            "extra": "mean: 1.5933658645999913 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ca90f59c52074ddf7cf0a1d9cd83d398dbc8caf7",
          "message": "rename Persist to WriteToTable (#51)\n\n* rename Persist to WriteToTable\r\n\r\n* update README",
          "timestamp": "2020-11-11T17:51:19+02:00",
          "tree_id": "1477cf944331c9e0638957382082e1f0e56f807d",
          "url": "https://github.com/mlrun/storey/commit/ca90f59c52074ddf7cf0a1d9cd83d398dbc8caf7"
        },
        "date": 1605110139764,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1094.4859747993942,
            "unit": "iter/sec",
            "range": "stddev: 0.00008035116600799471",
            "extra": "mean: 913.6709131273133 usec\nrounds: 518"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 684.1402260604531,
            "unit": "iter/sec",
            "range": "stddev: 0.00017171788693845413",
            "extra": "mean: 1.4616886449703317 msec\nrounds: 676"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.6857504981639386,
            "unit": "iter/sec",
            "range": "stddev: 0.009869361422092238",
            "extra": "mean: 372.3354052000104 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5385799505085965,
            "unit": "iter/sec",
            "range": "stddev: 0.02776544574835401",
            "extra": "mean: 1.8567345462000049 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3501.9650328623043,
            "unit": "iter/sec",
            "range": "stddev: 0.00005025604755569277",
            "extra": "mean: 285.5539648785864 usec\nrounds: 2591"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2357.493780882288,
            "unit": "iter/sec",
            "range": "stddev: 0.00007889253448095609",
            "extra": "mean: 424.17927381583667 usec\nrounds: 2173"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.003210054493799,
            "unit": "iter/sec",
            "range": "stddev: 0.0022683865088422624",
            "extra": "mean: 90.88256927273619 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.189118492057137,
            "unit": "iter/sec",
            "range": "stddev: 0.01535240917647097",
            "extra": "mean: 456.8048754000017 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1001.0237282791556,
            "unit": "iter/sec",
            "range": "stddev: 0.0004589781286952378",
            "extra": "mean: 998.9773186686439 usec\nrounds: 841"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 665.2436590503595,
            "unit": "iter/sec",
            "range": "stddev: 0.00034850943015303417",
            "extra": "mean: 1.5032086159641234 msec\nrounds: 664"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.5233023044077325,
            "unit": "iter/sec",
            "range": "stddev: 0.009341334471069877",
            "extra": "mean: 396.30606219999436 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.49647464672979347,
            "unit": "iter/sec",
            "range": "stddev: 0.017047646646330128",
            "extra": "mean: 2.0142015439999907 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1034.1900931806072,
            "unit": "iter/sec",
            "range": "stddev: 0.0002523193450294746",
            "extra": "mean: 966.9402236532193 usec\nrounds: 854"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 375.68060682868446,
            "unit": "iter/sec",
            "range": "stddev: 0.0010098172135345304",
            "extra": "mean: 2.661835564101965 msec\nrounds: 351"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7708215939195877,
            "unit": "iter/sec",
            "range": "stddev: 0.004508553936025712",
            "extra": "mean: 1.2973170547999984 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1510574342147367,
            "unit": "iter/sec",
            "range": "stddev: 0.04444724460552279",
            "extra": "mean: 6.619998579999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1009.08352783975,
            "unit": "iter/sec",
            "range": "stddev: 0.00038328419132361457",
            "extra": "mean: 990.9982398987366 usec\nrounds: 396"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 667.8454695739719,
            "unit": "iter/sec",
            "range": "stddev: 0.00030015923570226227",
            "extra": "mean: 1.4973523750006335 msec\nrounds: 624"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 2.732266472821095,
            "unit": "iter/sec",
            "range": "stddev: 0.027889325472392423",
            "extra": "mean: 365.99651240001094 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.5595724828090848,
            "unit": "iter/sec",
            "range": "stddev: 0.04657188540102445",
            "extra": "mean: 1.7870785836000096 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e1602c7a07fbf8cfb6548a84cd44bde22202de1a",
          "message": "WriteToCSV API overhaul. (#52)\n\n* WriteToCSV API overhaul.\r\n\r\n* WriteToCSV fixes.\r\n\r\n* Allow metadata_columns when columns are inferred.\r\n\r\n* Support metadata insertion into list events.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-12T12:45:49+02:00",
          "tree_id": "397b86ae9286533622ee9d26bb37cef4601849f5",
          "url": "https://github.com/mlrun/storey/commit/e1602c7a07fbf8cfb6548a84cd44bde22202de1a"
        },
        "date": 1605178225424,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1302.7043681920727,
            "unit": "iter/sec",
            "range": "stddev: 0.00019949249256511956",
            "extra": "mean: 767.6338733613262 usec\nrounds: 229"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 854.3563788255872,
            "unit": "iter/sec",
            "range": "stddev: 0.00025767281696968783",
            "extra": "mean: 1.1704717431554932 msec\nrounds: 767"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.067229120582145,
            "unit": "iter/sec",
            "range": "stddev: 0.00500153666927816",
            "extra": "mean: 245.8676338000032 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.80700366341363,
            "unit": "iter/sec",
            "range": "stddev: 0.021956592016484183",
            "extra": "mean: 1.239151747799997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5032.8114237576065,
            "unit": "iter/sec",
            "range": "stddev: 0.000017178984111059205",
            "extra": "mean: 198.69609961530773 usec\nrounds: 2600"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3317.877213450239,
            "unit": "iter/sec",
            "range": "stddev: 0.000047536732736829566",
            "extra": "mean: 301.39753091106905 usec\nrounds: 2119"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.738524427669313,
            "unit": "iter/sec",
            "range": "stddev: 0.0007460832607111191",
            "extra": "mean: 67.84939733333508 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.0459059512086473,
            "unit": "iter/sec",
            "range": "stddev: 0.0072906916178474765",
            "extra": "mean: 328.309545999997 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1395.2161615400166,
            "unit": "iter/sec",
            "range": "stddev: 0.00005977385100996028",
            "extra": "mean: 716.7348168445931 usec\nrounds: 748"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 855.2203478832411,
            "unit": "iter/sec",
            "range": "stddev: 0.00006829248600501676",
            "extra": "mean: 1.169289297752449 msec\nrounds: 890"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.7193594685156457,
            "unit": "iter/sec",
            "range": "stddev: 0.0015217861809878083",
            "extra": "mean: 268.8634987999933 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7707338029505563,
            "unit": "iter/sec",
            "range": "stddev: 0.012027631423494424",
            "extra": "mean: 1.2974648265999975 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1302.3650257218283,
            "unit": "iter/sec",
            "range": "stddev: 0.000049435453822743494",
            "extra": "mean: 767.8338870054927 usec\nrounds: 1062"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 439.0724285421615,
            "unit": "iter/sec",
            "range": "stddev: 0.0013295761248106074",
            "extra": "mean: 2.2775285693075036 msec\nrounds: 404"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9151486311277158,
            "unit": "iter/sec",
            "range": "stddev: 0.02204780048439303",
            "extra": "mean: 1.0927186753999991 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.18117475131700445,
            "unit": "iter/sec",
            "range": "stddev: 0.020922789942687495",
            "extra": "mean: 5.5195328969999995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1403.3085392092614,
            "unit": "iter/sec",
            "range": "stddev: 0.00007594338765128961",
            "extra": "mean: 712.6016638960108 usec\nrounds: 1083"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 889.2557569001954,
            "unit": "iter/sec",
            "range": "stddev: 0.00012783258117452526",
            "extra": "mean: 1.1245358742302007 msec\nrounds: 811"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.518970204452547,
            "unit": "iter/sec",
            "range": "stddev: 0.0027088306642152012",
            "extra": "mean: 221.28935460001458 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9224442126878013,
            "unit": "iter/sec",
            "range": "stddev: 0.01000872695769617",
            "extra": "mean: 1.0840763985999957 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "496b4ae4eb2ce3d702a397309724b9c5fa87c3f3",
          "message": "rename joinWithHttp -> SendToHttp (#53)",
          "timestamp": "2020-11-12T13:56:36+02:00",
          "tree_id": "69662b40636851f2007b550ed56e23ac5eda775b",
          "url": "https://github.com/mlrun/storey/commit/496b4ae4eb2ce3d702a397309724b9c5fa87c3f3"
        },
        "date": 1605182425953,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1295.4370246479218,
            "unit": "iter/sec",
            "range": "stddev: 0.00005317004547117737",
            "extra": "mean: 771.940264924714 usec\nrounds: 268"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 822.8153466403252,
            "unit": "iter/sec",
            "range": "stddev: 0.00008156326565827253",
            "extra": "mean: 1.2153395097492354 msec\nrounds: 718"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.969131996953759,
            "unit": "iter/sec",
            "range": "stddev: 0.006692087376860637",
            "extra": "mean: 251.94425399998866 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8087482128552886,
            "unit": "iter/sec",
            "range": "stddev: 0.014517045947750733",
            "extra": "mean: 1.2364787756000055 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4631.182462049659,
            "unit": "iter/sec",
            "range": "stddev: 0.000009221533478185074",
            "extra": "mean: 215.92757534269597 usec\nrounds: 2409"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3097.4411086136734,
            "unit": "iter/sec",
            "range": "stddev: 0.000011029735174982371",
            "extra": "mean: 322.84713895579813 usec\nrounds: 2087"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.732731721320901,
            "unit": "iter/sec",
            "range": "stddev: 0.00010729388344029126",
            "extra": "mean: 72.81872392857126 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.751231523862575,
            "unit": "iter/sec",
            "range": "stddev: 0.0007119247456193515",
            "extra": "mean: 363.4735904000024 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1292.8188103316365,
            "unit": "iter/sec",
            "range": "stddev: 0.00007713802055640333",
            "extra": "mean: 773.5035969529852 usec\nrounds: 722"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 804.2716338800157,
            "unit": "iter/sec",
            "range": "stddev: 0.000058927672105698685",
            "extra": "mean: 1.2433610211710935 msec\nrounds: 803"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.6180406523134474,
            "unit": "iter/sec",
            "range": "stddev: 0.0024553482746558267",
            "extra": "mean: 276.3926932000004 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.727073350117585,
            "unit": "iter/sec",
            "range": "stddev: 0.009310650506264131",
            "extra": "mean: 1.3753770508000003 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1232.2727562825492,
            "unit": "iter/sec",
            "range": "stddev: 0.00030344132108614763",
            "extra": "mean: 811.5086492837378 usec\nrounds: 978"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 416.15711880408946,
            "unit": "iter/sec",
            "range": "stddev: 0.001302149119523761",
            "extra": "mean: 2.4029385893330377 msec\nrounds: 375"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8585750517797305,
            "unit": "iter/sec",
            "range": "stddev: 0.007039848536733805",
            "extra": "mean: 1.1647205424000049 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16808677360278396,
            "unit": "iter/sec",
            "range": "stddev: 0.02424056043132612",
            "extra": "mean: 5.949308078000001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1278.1234805985107,
            "unit": "iter/sec",
            "range": "stddev: 0.00021897958997735325",
            "extra": "mean: 782.3970181126216 usec\nrounds: 1049"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 814.5343442659785,
            "unit": "iter/sec",
            "range": "stddev: 0.00006889830057647124",
            "extra": "mean: 1.22769531701104 msec\nrounds: 776"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.333666964440126,
            "unit": "iter/sec",
            "range": "stddev: 0.00594535542836602",
            "extra": "mean: 230.7514648000165 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8745542079957288,
            "unit": "iter/sec",
            "range": "stddev: 0.01850692392584874",
            "extra": "mean: 1.1434396986000024 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "00d1e60d846b68da931f03ca4e3df7207bd256bd",
          "message": "WriteToParquet - support metadata columns, column inference. (#54)\n\n* WriteToParquet - support metadata columns.\r\n\r\n* Update WriteToParquet docstring.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-12T18:17:33+02:00",
          "tree_id": "25ab0093dff4bd4b48459b1346c324a8afed15b8",
          "url": "https://github.com/mlrun/storey/commit/00d1e60d846b68da931f03ca4e3df7207bd256bd"
        },
        "date": 1605198075891,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1318.1023672812303,
            "unit": "iter/sec",
            "range": "stddev: 0.00013615413400501765",
            "extra": "mean: 758.6664168297028 usec\nrounds: 511"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 867.0834629526527,
            "unit": "iter/sec",
            "range": "stddev: 0.00010075945435552428",
            "extra": "mean: 1.153291514284831 msec\nrounds: 700"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.066932484898569,
            "unit": "iter/sec",
            "range": "stddev: 0.007428978875459683",
            "extra": "mean: 245.88556699999913 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7912734811792237,
            "unit": "iter/sec",
            "range": "stddev: 0.017485642904191422",
            "extra": "mean: 1.2637855606000017 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4987.023122346045,
            "unit": "iter/sec",
            "range": "stddev: 0.00002018916475640753",
            "extra": "mean: 200.52042580656214 usec\nrounds: 2480"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3324.034135815541,
            "unit": "iter/sec",
            "range": "stddev: 0.00005824568932556106",
            "extra": "mean: 300.8392691354396 usec\nrounds: 2025"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.525684866504475,
            "unit": "iter/sec",
            "range": "stddev: 0.0010549498114035335",
            "extra": "mean: 68.84356979999968 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.9026026064361097,
            "unit": "iter/sec",
            "range": "stddev: 0.0024091660671739265",
            "extra": "mean: 344.51839800000243 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1307.9556003142984,
            "unit": "iter/sec",
            "range": "stddev: 0.0001773276769799627",
            "extra": "mean: 764.5519463808272 usec\nrounds: 746"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 835.4374330582151,
            "unit": "iter/sec",
            "range": "stddev: 0.00010530500380936559",
            "extra": "mean: 1.1969777273917264 msec\nrounds: 763"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.5386897863482765,
            "unit": "iter/sec",
            "range": "stddev: 0.005828229973282481",
            "extra": "mean: 282.5904671999922 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7311737079990877,
            "unit": "iter/sec",
            "range": "stddev: 0.02154460321896915",
            "extra": "mean: 1.3676640571999996 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1271.0447976501878,
            "unit": "iter/sec",
            "range": "stddev: 0.0000984746818917709",
            "extra": "mean: 786.7543314356228 usec\nrounds: 878"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 420.8143468281714,
            "unit": "iter/sec",
            "range": "stddev: 0.001709701573575326",
            "extra": "mean: 2.376344836000385 msec\nrounds: 250"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9019040818233861,
            "unit": "iter/sec",
            "range": "stddev: 0.021974622292022232",
            "extra": "mean: 1.1087653556000021 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17581255681974817,
            "unit": "iter/sec",
            "range": "stddev: 0.07913956724352392",
            "extra": "mean: 5.687875872400002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1351.21055042824,
            "unit": "iter/sec",
            "range": "stddev: 0.00006569771450402442",
            "extra": "mean: 740.0771106198581 usec\nrounds: 904"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 852.7646519160037,
            "unit": "iter/sec",
            "range": "stddev: 0.00018071717625853346",
            "extra": "mean: 1.1726564858817563 msec\nrounds: 850"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.340636743161458,
            "unit": "iter/sec",
            "range": "stddev: 0.006871712054330203",
            "extra": "mean: 230.3809461999947 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8628851751504418,
            "unit": "iter/sec",
            "range": "stddev: 0.009508378779023984",
            "extra": "mean: 1.1589027471999997 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8c3718b79eeff3a9e07a15db109d8142379ae820",
          "message": "Support metadata columns and column inference in WriteToTSDB. (#55)\n\n* Support metadata columns and column inference in WriteToTSDB.\r\n\r\n* WriteToTSDB unit test.\r\n\r\n* Update type annotation.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-16T10:24:51+02:00",
          "tree_id": "7c1b0c5b7bbbe585b93ac8489ff5961c56d8bee7",
          "url": "https://github.com/mlrun/storey/commit/8c3718b79eeff3a9e07a15db109d8142379ae820"
        },
        "date": 1605515321883,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1163.6102154583323,
            "unit": "iter/sec",
            "range": "stddev: 0.000660393083925407",
            "extra": "mean: 859.3943115273457 usec\nrounds: 321"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 778.5473429027359,
            "unit": "iter/sec",
            "range": "stddev: 0.0007348619119802818",
            "extra": "mean: 1.2844434049079148 msec\nrounds: 815"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.606298154209683,
            "unit": "iter/sec",
            "range": "stddev: 0.026164978090108943",
            "extra": "mean: 277.2926577999897 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7732131469209929,
            "unit": "iter/sec",
            "range": "stddev: 0.06924792934253315",
            "extra": "mean: 1.293304445199999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4840.84760948886,
            "unit": "iter/sec",
            "range": "stddev: 0.000010546180953299278",
            "extra": "mean: 206.57539354055166 usec\nrounds: 2508"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3252.7353971167304,
            "unit": "iter/sec",
            "range": "stddev: 0.000036406250337326074",
            "extra": "mean: 307.4335529678848 usec\nrounds: 2190"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.368474881021307,
            "unit": "iter/sec",
            "range": "stddev: 0.00041298757025043565",
            "extra": "mean: 69.59680886667077 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.871274754655773,
            "unit": "iter/sec",
            "range": "stddev: 0.0007759321811213716",
            "extra": "mean: 348.2773630000054 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1063.918003443399,
            "unit": "iter/sec",
            "range": "stddev: 0.0010716740245049683",
            "extra": "mean: 939.9220586205643 usec\nrounds: 870"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 773.8175227406725,
            "unit": "iter/sec",
            "range": "stddev: 0.0006580311287279105",
            "extra": "mean: 1.2922943337574526 msec\nrounds: 785"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.202306357399223,
            "unit": "iter/sec",
            "range": "stddev: 0.02252405300714649",
            "extra": "mean: 312.27493199999685 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6945400958311941,
            "unit": "iter/sec",
            "range": "stddev: 0.06784516434105886",
            "extra": "mean: 1.439801684600002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1224.285006708724,
            "unit": "iter/sec",
            "range": "stddev: 0.000366704243960486",
            "extra": "mean: 816.8032725389041 usec\nrounds: 965"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 415.6790448447406,
            "unit": "iter/sec",
            "range": "stddev: 0.0013289646586419179",
            "extra": "mean: 2.4057022176172196 msec\nrounds: 386"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8271792261830642,
            "unit": "iter/sec",
            "range": "stddev: 0.034081581930810026",
            "extra": "mean: 1.2089278457999968 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16617338815370114,
            "unit": "iter/sec",
            "range": "stddev: 0.06419094997792353",
            "extra": "mean: 6.0178107403999945 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1234.688989026533,
            "unit": "iter/sec",
            "range": "stddev: 0.0002436630640384447",
            "extra": "mean: 809.9205620910501 usec\nrounds: 1071"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 805.8693908823891,
            "unit": "iter/sec",
            "range": "stddev: 0.0007725560050877077",
            "extra": "mean: 1.2408958713583191 msec\nrounds: 824"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.9853173448500874,
            "unit": "iter/sec",
            "range": "stddev: 0.01847643583134333",
            "extra": "mean: 250.9210468000049 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8426421375347111,
            "unit": "iter/sec",
            "range": "stddev: 0.08962724067484723",
            "extra": "mean: 1.1867434055999921 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6d9f7794e8220e355a6f6fd6e04787f6a3894e96",
          "message": "add support for passing aggregations as dict (#57)\n\n* add support for passing aggregations as dict\r\n\r\n* deal with empty aggr list",
          "timestamp": "2020-11-17T15:18:31+02:00",
          "tree_id": "4b4564ab9b19457e1b4a64460804f33d21d864ab",
          "url": "https://github.com/mlrun/storey/commit/6d9f7794e8220e355a6f6fd6e04787f6a3894e96"
        },
        "date": 1605619398720,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1032.4001715489205,
            "unit": "iter/sec",
            "range": "stddev: 0.0006153441084454285",
            "extra": "mean: 968.6166542375616 usec\nrounds: 590"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 641.3728316363563,
            "unit": "iter/sec",
            "range": "stddev: 0.0010955273430317482",
            "extra": "mean: 1.5591555343070365 msec\nrounds: 685"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.005957631877224,
            "unit": "iter/sec",
            "range": "stddev: 0.026321176322942466",
            "extra": "mean: 332.67268620000436 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5575469924869723,
            "unit": "iter/sec",
            "range": "stddev: 0.130518212950298",
            "extra": "mean: 1.793570790399997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4353.446386695022,
            "unit": "iter/sec",
            "range": "stddev: 0.00010344794940602583",
            "extra": "mean: 229.7030699760526 usec\nrounds: 1672"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2899.0221959026258,
            "unit": "iter/sec",
            "range": "stddev: 0.00023228944252906856",
            "extra": "mean: 344.9438922590397 usec\nrounds: 1912"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.241061042723462,
            "unit": "iter/sec",
            "range": "stddev: 0.0049825258478931106",
            "extra": "mean: 75.52264858332809 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.645558296011217,
            "unit": "iter/sec",
            "range": "stddev: 0.005671739786545042",
            "extra": "mean: 377.99204859999804 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 931.2231444393052,
            "unit": "iter/sec",
            "range": "stddev: 0.001149159296064258",
            "extra": "mean: 1.0738564714283445 msec\nrounds: 770"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 627.1174401794204,
            "unit": "iter/sec",
            "range": "stddev: 0.0006914815810765833",
            "extra": "mean: 1.594597655765875 msec\nrounds: 581"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.5026266505707975,
            "unit": "iter/sec",
            "range": "stddev: 0.051163795286239926",
            "extra": "mean: 399.5801770000014 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5316514327880077,
            "unit": "iter/sec",
            "range": "stddev: 0.06727234390580379",
            "extra": "mean: 1.8809316373999934 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 960.3664674950097,
            "unit": "iter/sec",
            "range": "stddev: 0.0009120977649571331",
            "extra": "mean: 1.0412691757224397 msec\nrounds: 865"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 339.34160822883285,
            "unit": "iter/sec",
            "range": "stddev: 0.0021561049284172565",
            "extra": "mean: 2.9468829514288637 msec\nrounds: 350"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.6992865603132624,
            "unit": "iter/sec",
            "range": "stddev: 0.03470526095422273",
            "extra": "mean: 1.4300289134000024 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.13872334754764667,
            "unit": "iter/sec",
            "range": "stddev: 0.09883418289423962",
            "extra": "mean: 7.208591903800004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1008.8271598762052,
            "unit": "iter/sec",
            "range": "stddev: 0.0011286396702660266",
            "extra": "mean: 991.2500770922064 usec\nrounds: 908"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 668.4286008256302,
            "unit": "iter/sec",
            "range": "stddev: 0.0010105948001430545",
            "extra": "mean: 1.4960460979150492 msec\nrounds: 623"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.185664450845056,
            "unit": "iter/sec",
            "range": "stddev: 0.022460382278746305",
            "extra": "mean: 313.9062558000205 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6113665366264632,
            "unit": "iter/sec",
            "range": "stddev: 0.03216077162190215",
            "extra": "mean: 1.6356799727999942 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "78d15f095c3dfb524699a36602c4e8b72521e3bd",
          "message": "support passing key as string to joinWithTable (#59)\n\n* support passing key as string to joinWithTable\r\n\r\n* update doc",
          "timestamp": "2020-11-18T10:13:53+02:00",
          "tree_id": "a514b50922321ee0c693fb44451420c63347ed5d",
          "url": "https://github.com/mlrun/storey/commit/78d15f095c3dfb524699a36602c4e8b72521e3bd"
        },
        "date": 1605687438025,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1389.716417183084,
            "unit": "iter/sec",
            "range": "stddev: 0.00005519568091723263",
            "extra": "mean: 719.5712647814665 usec\nrounds: 778"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 860.9840553026352,
            "unit": "iter/sec",
            "range": "stddev: 0.00008422432663525599",
            "extra": "mean: 1.1614616947215135 msec\nrounds: 701"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.753587164370234,
            "unit": "iter/sec",
            "range": "stddev: 0.004702978833453017",
            "extra": "mean: 266.4118232000021 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8473492577550276,
            "unit": "iter/sec",
            "range": "stddev: 0.011883081740886935",
            "extra": "mean: 1.180150912800002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5400.808570780843,
            "unit": "iter/sec",
            "range": "stddev: 0.000021610157068228152",
            "extra": "mean: 185.15746057176415 usec\nrounds: 2625"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3571.099572169457,
            "unit": "iter/sec",
            "range": "stddev: 0.0001353556917242375",
            "extra": "mean: 280.0257959182292 usec\nrounds: 2401"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 16.667498391177855,
            "unit": "iter/sec",
            "range": "stddev: 0.002459815358557934",
            "extra": "mean: 59.99700594117365 msec\nrounds: 17"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.453378891477224,
            "unit": "iter/sec",
            "range": "stddev: 0.0052265920630423564",
            "extra": "mean: 289.57146939999916 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1410.5134826234291,
            "unit": "iter/sec",
            "range": "stddev: 0.00022409003513256857",
            "extra": "mean: 708.961674113238 usec\nrounds: 761"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 874.0010088751999,
            "unit": "iter/sec",
            "range": "stddev: 0.00009486811956907403",
            "extra": "mean: 1.144163438995288 msec\nrounds: 836"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.079187910832641,
            "unit": "iter/sec",
            "range": "stddev: 0.005140667876036899",
            "extra": "mean: 245.1468335999948 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.810542225245366,
            "unit": "iter/sec",
            "range": "stddev: 0.011039442099049543",
            "extra": "mean: 1.2337420172000066 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1338.7854104916257,
            "unit": "iter/sec",
            "range": "stddev: 0.00007166691594685874",
            "extra": "mean: 746.9456958249809 usec\nrounds: 1006"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 469.0722651804562,
            "unit": "iter/sec",
            "range": "stddev: 0.0011570277341023715",
            "extra": "mean: 2.1318676763276363 msec\nrounds: 414"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9873292927695576,
            "unit": "iter/sec",
            "range": "stddev: 0.020260375734080602",
            "extra": "mean: 1.012833314400001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.19343634676546942,
            "unit": "iter/sec",
            "range": "stddev: 0.060834229413837336",
            "extra": "mean: 5.169659253399999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1378.6141846386029,
            "unit": "iter/sec",
            "range": "stddev: 0.00031923559330516136",
            "extra": "mean: 725.3661039779199 usec\nrounds: 1106"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 910.777099823321,
            "unit": "iter/sec",
            "range": "stddev: 0.00010132926968590217",
            "extra": "mean: 1.0979634865588814 msec\nrounds: 744"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.603545128784501,
            "unit": "iter/sec",
            "range": "stddev: 0.0024697375585239355",
            "extra": "mean: 217.22389419999786 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9166208406354952,
            "unit": "iter/sec",
            "range": "stddev: 0.022672730117459947",
            "extra": "mean: 1.0909636304000003 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9220c4dc1e62ba971fe679bd7d30e8a371683432",
          "message": "Writer API changes. (#60)\n\n* Writer API changes.\r\n\r\n* Added test.\r\n\r\n* Support rename, better $ syntax.\r\n\r\n* Update documentation.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-18T11:20:35+02:00",
          "tree_id": "54bc9c5973f2d154e609446a040306a1653eb7ed",
          "url": "https://github.com/mlrun/storey/commit/9220c4dc1e62ba971fe679bd7d30e8a371683432"
        },
        "date": 1605691478485,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1178.9608514316276,
            "unit": "iter/sec",
            "range": "stddev: 0.00014840561784799946",
            "extra": "mean: 848.2045852376582 usec\nrounds: 569"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 681.0574373748105,
            "unit": "iter/sec",
            "range": "stddev: 0.000853163280562617",
            "extra": "mean: 1.4683049404094002 msec\nrounds: 537"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.919118992746974,
            "unit": "iter/sec",
            "range": "stddev: 0.008571086165771472",
            "extra": "mean: 342.5691115999939 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5968780992469992,
            "unit": "iter/sec",
            "range": "stddev: 0.01505229519718454",
            "extra": "mean: 1.67538397080001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3551.158083461062,
            "unit": "iter/sec",
            "range": "stddev: 0.0002520135652073219",
            "extra": "mean: 281.5982776597123 usec\nrounds: 2820"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2465.0780103110965,
            "unit": "iter/sec",
            "range": "stddev: 0.0002474618287989966",
            "extra": "mean: 405.66667497626116 usec\nrounds: 2126"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.342655555051818,
            "unit": "iter/sec",
            "range": "stddev: 0.0038689967287055776",
            "extra": "mean: 88.1627759166695 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.2115676569109426,
            "unit": "iter/sec",
            "range": "stddev: 0.017628763808069863",
            "extra": "mean: 452.16794379999783 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1085.1772041141708,
            "unit": "iter/sec",
            "range": "stddev: 0.0005896389942651021",
            "extra": "mean: 921.5084837838068 usec\nrounds: 370"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 716.4179486181935,
            "unit": "iter/sec",
            "range": "stddev: 0.0005703272466213296",
            "extra": "mean: 1.3958332589639493 msec\nrounds: 753"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.660578427448305,
            "unit": "iter/sec",
            "range": "stddev: 0.010261560158741405",
            "extra": "mean: 375.8581177999986 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5368200252633206,
            "unit": "iter/sec",
            "range": "stddev: 0.020619590345851594",
            "extra": "mean: 1.8628217148000032 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1118.1658933182746,
            "unit": "iter/sec",
            "range": "stddev: 0.00027378991238776753",
            "extra": "mean: 894.3216797933221 usec\nrounds: 965"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 377.9496905320524,
            "unit": "iter/sec",
            "range": "stddev: 0.0013476028393678808",
            "extra": "mean: 2.64585479245205 msec\nrounds: 371"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8042456856785101,
            "unit": "iter/sec",
            "range": "stddev: 0.020514448050443036",
            "extra": "mean: 1.243401136999995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15797371871947283,
            "unit": "iter/sec",
            "range": "stddev: 0.04414568265766886",
            "extra": "mean: 6.330166866399997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1116.4458100424868,
            "unit": "iter/sec",
            "range": "stddev: 0.00044310101908898376",
            "extra": "mean: 895.6995413525217 usec\nrounds: 931"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 688.175159118153,
            "unit": "iter/sec",
            "range": "stddev: 0.0006248821863197758",
            "extra": "mean: 1.453118420143831 msec\nrounds: 695"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.152584153747766,
            "unit": "iter/sec",
            "range": "stddev: 0.006850548886746546",
            "extra": "mean: 317.20009720000917 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6256995365399615,
            "unit": "iter/sec",
            "range": "stddev: 0.017323610062547422",
            "extra": "mean: 1.5982111885999983 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "349b368887015602ef9a1928a27e9b669c540522",
          "message": "support passing emit_policy as dict (#62)\n\n* support passing emit_policy as dict\r\n\r\n* Dict typing + bad arguments handling when parsing emit policy\r\n\r\n* more typing annotations\r\n\r\n* more typing and delete comment line\r\n\r\n* add missing Optional\r\n\r\n* typo",
          "timestamp": "2020-11-18T17:22:13+02:00",
          "tree_id": "8d4c72ad368b5a0e057acb0e9049a8b18204a3b7",
          "url": "https://github.com/mlrun/storey/commit/349b368887015602ef9a1928a27e9b669c540522"
        },
        "date": 1605713186077,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1259.962152053851,
            "unit": "iter/sec",
            "range": "stddev: 0.00016482179605849037",
            "extra": "mean: 793.6746340911197 usec\nrounds: 440"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 828.2356814354035,
            "unit": "iter/sec",
            "range": "stddev: 0.00016949294201306484",
            "extra": "mean: 1.2073857990118393 msec\nrounds: 607"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.9916544468685564,
            "unit": "iter/sec",
            "range": "stddev: 0.007855029105934693",
            "extra": "mean: 250.52268759999947 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7576218912389556,
            "unit": "iter/sec",
            "range": "stddev: 0.0766716700803432",
            "extra": "mean: 1.3199196216000018 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4824.933494874124,
            "unit": "iter/sec",
            "range": "stddev: 0.000039110351685879776",
            "extra": "mean: 207.256742722438 usec\nrounds: 2336"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3013.937584567351,
            "unit": "iter/sec",
            "range": "stddev: 0.00010148330827634235",
            "extra": "mean: 331.79187423138006 usec\nrounds: 1789"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.424674622228556,
            "unit": "iter/sec",
            "range": "stddev: 0.002982026207685921",
            "extra": "mean: 69.32565386667306 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.785961240203953,
            "unit": "iter/sec",
            "range": "stddev: 0.02730147099330938",
            "extra": "mean: 358.94253859999594 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1203.158166202988,
            "unit": "iter/sec",
            "range": "stddev: 0.0001252893504024365",
            "extra": "mean: 831.1459192068413 usec\nrounds: 656"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 769.2280944848144,
            "unit": "iter/sec",
            "range": "stddev: 0.0001563189823125604",
            "extra": "mean: 1.300004520336382 msec\nrounds: 713"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.301006835275334,
            "unit": "iter/sec",
            "range": "stddev: 0.011378992261068924",
            "extra": "mean: 302.93787620000217 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.673142775767358,
            "unit": "iter/sec",
            "range": "stddev: 0.041378802021195885",
            "extra": "mean: 1.4855689401999996 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1185.3567322461074,
            "unit": "iter/sec",
            "range": "stddev: 0.00009137254881400239",
            "extra": "mean: 843.6278909093646 usec\nrounds: 935"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 404.4323997045886,
            "unit": "iter/sec",
            "range": "stddev: 0.0015175297681225214",
            "extra": "mean: 2.472601108937945 msec\nrounds: 358"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8776045180046683,
            "unit": "iter/sec",
            "range": "stddev: 0.017212697590917883",
            "extra": "mean: 1.1394654191999962 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.165004964339687,
            "unit": "iter/sec",
            "range": "stddev: 0.09602750787356748",
            "extra": "mean: 6.060423721200005 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1162.0980930471717,
            "unit": "iter/sec",
            "range": "stddev: 0.000325897963402947",
            "extra": "mean: 860.5125556809671 usec\nrounds: 880"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 714.0511998711254,
            "unit": "iter/sec",
            "range": "stddev: 0.0005945012985623403",
            "extra": "mean: 1.4004597992139551 msec\nrounds: 762"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.725971542068338,
            "unit": "iter/sec",
            "range": "stddev: 0.01812110969320746",
            "extra": "mean: 268.38637619998735 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7518180171585457,
            "unit": "iter/sec",
            "range": "stddev: 0.09756368332986878",
            "extra": "mean: 1.3301091184000142 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33829179+talIguaz@users.noreply.github.com",
            "name": "Tal Neiman",
            "username": "talIguaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "69e4c770f7583ffe7baf68274138da341c40eb2e",
          "message": "Select columns to persist in write table (#61)\n\n* support requesting specific columns to persist in WriteToTable\r\n\r\n* get columns from the events body\r\n\r\n* use last event of batch in WriteToTable\r\n\r\n* add docs\r\n\r\n* use _Writer in WriteToTable\r\n\r\n* update doc\r\n\r\n* typing and formatting",
          "timestamp": "2020-11-18T17:56:33+02:00",
          "tree_id": "f435b9a527fa822a8331dbde0f45ea2761f9ead9",
          "url": "https://github.com/mlrun/storey/commit/69e4c770f7583ffe7baf68274138da341c40eb2e"
        },
        "date": 1605715275050,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1065.7057701253896,
            "unit": "iter/sec",
            "range": "stddev: 0.0002101990839475487",
            "extra": "mean: 938.3452994557224 usec\nrounds: 551"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 640.6726630384052,
            "unit": "iter/sec",
            "range": "stddev: 0.000701903852643853",
            "extra": "mean: 1.5608594804989437 msec\nrounds: 641"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 2.6608089985409915,
            "unit": "iter/sec",
            "range": "stddev: 0.026084859770923837",
            "extra": "mean: 375.82554800000025 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5238255947737824,
            "unit": "iter/sec",
            "range": "stddev: 0.09504503118785766",
            "extra": "mean: 1.9090323382000007 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3413.0021492771407,
            "unit": "iter/sec",
            "range": "stddev: 0.00004564469187827401",
            "extra": "mean: 292.99717851387686 usec\nrounds: 2476"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2243.2635326326285,
            "unit": "iter/sec",
            "range": "stddev: 0.00009474590860868195",
            "extra": "mean: 445.7791006063515 usec\nrounds: 1978"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 10.169785389912878,
            "unit": "iter/sec",
            "range": "stddev: 0.0039454876778222585",
            "extra": "mean: 98.33049190908903 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.0276529846019162,
            "unit": "iter/sec",
            "range": "stddev: 0.015904103268989216",
            "extra": "mean: 493.18103620000215 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 999.4509564189589,
            "unit": "iter/sec",
            "range": "stddev: 0.00039881903877633166",
            "extra": "mean: 1.0005493451954945 msec\nrounds: 843"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 624.0809114570611,
            "unit": "iter/sec",
            "range": "stddev: 0.0005533887815150856",
            "extra": "mean: 1.6023563317539529 msec\nrounds: 633"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.355686952988856,
            "unit": "iter/sec",
            "range": "stddev: 0.02405639175991716",
            "extra": "mean: 424.5046221999985 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.44736023998991503,
            "unit": "iter/sec",
            "range": "stddev: 0.055894437947223596",
            "extra": "mean: 2.235334995400001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 984.2636702986943,
            "unit": "iter/sec",
            "range": "stddev: 0.0003902810635477151",
            "extra": "mean: 1.0159879208957596 msec\nrounds: 670"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 357.1999567991042,
            "unit": "iter/sec",
            "range": "stddev: 0.0015804912867032385",
            "extra": "mean: 2.7995524102552407 msec\nrounds: 351"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7220400686573423,
            "unit": "iter/sec",
            "range": "stddev: 0.019595193483606332",
            "extra": "mean: 1.3849646901999961 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14066225358949497,
            "unit": "iter/sec",
            "range": "stddev: 0.10000240028988451",
            "extra": "mean: 7.109227774199991 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1069.545673548249,
            "unit": "iter/sec",
            "range": "stddev: 0.00032790279338750547",
            "extra": "mean: 934.976434136254 usec\nrounds: 873"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 661.9729951659915,
            "unit": "iter/sec",
            "range": "stddev: 0.0003220253185311386",
            "extra": "mean: 1.5106356411854043 msec\nrounds: 641"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 2.8706427254695583,
            "unit": "iter/sec",
            "range": "stddev: 0.011089088470338071",
            "extra": "mean: 348.35404320000407 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.5794123042297412,
            "unit": "iter/sec",
            "range": "stddev: 0.028687615888538458",
            "extra": "mean: 1.7258867177999946 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "242a03cfc0f25cf76c3128cd779e6b4d98211536",
          "message": "Docstring and minor API changes. (#63)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-19T13:31:30+02:00",
          "tree_id": "3a02496331eacc9d39e961ec346ab1c32a2c93c2",
          "url": "https://github.com/mlrun/storey/commit/242a03cfc0f25cf76c3128cd779e6b4d98211536"
        },
        "date": 1605785749028,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1189.5454625139782,
            "unit": "iter/sec",
            "range": "stddev: 0.00032675750787630015",
            "extra": "mean: 840.6572354843893 usec\nrounds: 620"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 812.7008866002354,
            "unit": "iter/sec",
            "range": "stddev: 0.0003805195447533459",
            "extra": "mean: 1.2304650043920726 msec\nrounds: 683"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.1300419125946233,
            "unit": "iter/sec",
            "range": "stddev: 0.01181504513539489",
            "extra": "mean: 319.4845397999984 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7702625601044255,
            "unit": "iter/sec",
            "range": "stddev: 0.01683050296675719",
            "extra": "mean: 1.2982586091999964 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4262.999798430823,
            "unit": "iter/sec",
            "range": "stddev: 0.00004969710338464913",
            "extra": "mean: 234.5766003479738 usec\nrounds: 2875"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2897.653634201396,
            "unit": "iter/sec",
            "range": "stddev: 0.00009419052593279383",
            "extra": "mean: 345.1068092462347 usec\nrounds: 2574"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.013156684366567,
            "unit": "iter/sec",
            "range": "stddev: 0.0038240370843779866",
            "extra": "mean: 76.84530542857105 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.6794291621717274,
            "unit": "iter/sec",
            "range": "stddev: 0.005980222599871019",
            "extra": "mean: 373.2138225999904 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1249.5144277813579,
            "unit": "iter/sec",
            "range": "stddev: 0.00021876168306050355",
            "extra": "mean: 800.310886986398 usec\nrounds: 876"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 723.5856158132526,
            "unit": "iter/sec",
            "range": "stddev: 0.0003489241870322891",
            "extra": "mean: 1.382006466333745 msec\nrounds: 802"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.5471749602186935,
            "unit": "iter/sec",
            "range": "stddev: 0.015160262279702616",
            "extra": "mean: 392.59179899999594 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.49089141071413755,
            "unit": "iter/sec",
            "range": "stddev: 0.06036611442606464",
            "extra": "mean: 2.0371104039999866 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1059.6732270534267,
            "unit": "iter/sec",
            "range": "stddev: 0.00016134914502379487",
            "extra": "mean: 943.6871428569007 usec\nrounds: 931"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 381.9066289671962,
            "unit": "iter/sec",
            "range": "stddev: 0.00026013180094893893",
            "extra": "mean: 2.6184410642578686 msec\nrounds: 249"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7595763145967034,
            "unit": "iter/sec",
            "range": "stddev: 0.027333334397115213",
            "extra": "mean: 1.3165234101999999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15609592373800538,
            "unit": "iter/sec",
            "range": "stddev: 0.424112328968041",
            "extra": "mean: 6.406317192999995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1175.6051545294317,
            "unit": "iter/sec",
            "range": "stddev: 0.0006336980931019509",
            "extra": "mean: 850.6257361556716 usec\nrounds: 921"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 775.9288869529666,
            "unit": "iter/sec",
            "range": "stddev: 0.0002271053715196167",
            "extra": "mean: 1.2887778980970142 msec\nrounds: 736"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.6985903751134717,
            "unit": "iter/sec",
            "range": "stddev: 0.006269711582017789",
            "extra": "mean: 270.37327699997604 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7614784413098155,
            "unit": "iter/sec",
            "range": "stddev: 0.07709143699887261",
            "extra": "mean: 1.3132348150000213 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguaz.io",
            "name": "Gal Topper",
            "username": "gtopper"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7b376e75869f008e6d689d9f9746acd77243947c",
          "message": "Minor API changes. (#64)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-19T15:57:18+02:00",
          "tree_id": "b790ce1f7828ee8e3654008407d78e25754b72a1",
          "url": "https://github.com/mlrun/storey/commit/7b376e75869f008e6d689d9f9746acd77243947c"
        },
        "date": 1605794437067,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1583.417425442584,
            "unit": "iter/sec",
            "range": "stddev: 0.000024747324596516395",
            "extra": "mean: 631.5454054830096 usec\nrounds: 693"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 1023.6023769780772,
            "unit": "iter/sec",
            "range": "stddev: 0.00004550288460620604",
            "extra": "mean: 976.9418501667051 usec\nrounds: 901"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.525471843185179,
            "unit": "iter/sec",
            "range": "stddev: 0.01143966451306034",
            "extra": "mean: 220.97143340000684 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8901753289701813,
            "unit": "iter/sec",
            "range": "stddev: 0.020438439606611607",
            "extra": "mean: 1.1233742022000002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5535.666758255309,
            "unit": "iter/sec",
            "range": "stddev: 0.000008384062465211571",
            "extra": "mean: 180.6467122517275 usec\nrounds: 2669"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3714.3599148203116,
            "unit": "iter/sec",
            "range": "stddev: 0.00003345085251795659",
            "extra": "mean: 269.2253908970953 usec\nrounds: 2351"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 15.960643590646843,
            "unit": "iter/sec",
            "range": "stddev: 0.00011053758403139364",
            "extra": "mean: 62.654115062503735 msec\nrounds: 16"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.190600700660142,
            "unit": "iter/sec",
            "range": "stddev: 0.0009680941467351947",
            "extra": "mean: 313.4206044000109 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1569.9287328932128,
            "unit": "iter/sec",
            "range": "stddev: 0.000025342627690578258",
            "extra": "mean: 636.9715892498544 usec\nrounds: 986"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 1000.1707914108241,
            "unit": "iter/sec",
            "range": "stddev: 0.000047257151186588224",
            "extra": "mean: 999.8292377539008 usec\nrounds: 837"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.118532047536474,
            "unit": "iter/sec",
            "range": "stddev: 0.0016967273631608734",
            "extra": "mean: 242.80495779999 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8150272264498833,
            "unit": "iter/sec",
            "range": "stddev: 0.006204290639860764",
            "extra": "mean: 1.226952876599995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1501.4979975656308,
            "unit": "iter/sec",
            "range": "stddev: 0.00010776702764142454",
            "extra": "mean: 666.001554195406 usec\nrounds: 1144"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 494.79589990226276,
            "unit": "iter/sec",
            "range": "stddev: 0.0013626996107897961",
            "extra": "mean: 2.0210353404252754 msec\nrounds: 282"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.0728788708472397,
            "unit": "iter/sec",
            "range": "stddev: 0.002260157612731403",
            "extra": "mean: 932.0716692000019 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.21170228260609703,
            "unit": "iter/sec",
            "range": "stddev: 0.018720725438730667",
            "extra": "mean: 4.7236146331999915 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1564.7643442692329,
            "unit": "iter/sec",
            "range": "stddev: 0.000040725145056639274",
            "extra": "mean: 639.0738667214546 usec\nrounds: 1223"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 991.0403193282045,
            "unit": "iter/sec",
            "range": "stddev: 0.00012556338313425891",
            "extra": "mean: 1.0090406822982427 msec\nrounds: 853"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.71209812259764,
            "unit": "iter/sec",
            "range": "stddev: 0.011505737483751353",
            "extra": "mean: 212.21968940000124 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9221539511610217,
            "unit": "iter/sec",
            "range": "stddev: 0.04614772997589666",
            "extra": "mean: 1.0844176275999984 sec\nrounds: 5"
          }
        ]
      }
    ]
  }
}