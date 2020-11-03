window.BENCHMARK_DATA = {
  "lastUpdate": 1604418238558,
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
      }
    ]
  }
}