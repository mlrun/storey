window.BENCHMARK_DATA = {
  "lastUpdate": 1608138346396,
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
          "id": "c2e6e2c3a420b5a9c7f59ffd6b054ee86640e055",
          "message": "Writer API: separate index from columns. (#65)\n\n* Writer API: separate index from columns.\r\n\r\nAnd other API changes.\r\n\r\n* Fix integration tests.\r\n\r\n* CR touch ups.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-22T15:54:41+02:00",
          "tree_id": "b0ad785cdd5c6f41bbbcfd1af4e2db6fb21aaefe",
          "url": "https://github.com/mlrun/storey/commit/c2e6e2c3a420b5a9c7f59ffd6b054ee86640e055"
        },
        "date": 1606053511207,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1205.2065273868834,
            "unit": "iter/sec",
            "range": "stddev: 0.00021782862599238894",
            "extra": "mean: 829.7333090023914 usec\nrounds: 411"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 801.8925568717841,
            "unit": "iter/sec",
            "range": "stddev: 0.0006587957840434899",
            "extra": "mean: 1.2470498590248065 msec\nrounds: 759"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.4388701845109875,
            "unit": "iter/sec",
            "range": "stddev: 0.02074753122383943",
            "extra": "mean: 290.79318099999796 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7192258540252385,
            "unit": "iter/sec",
            "range": "stddev: 0.036763625086429184",
            "extra": "mean: 1.3903838334 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4700.80955115647,
            "unit": "iter/sec",
            "range": "stddev: 0.000011826719718858175",
            "extra": "mean: 212.7293159013398 usec\nrounds: 2352"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3133.4515957466965,
            "unit": "iter/sec",
            "range": "stddev: 0.00005463576815473",
            "extra": "mean: 319.1368908833269 usec\nrounds: 2117"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.02543838828903,
            "unit": "iter/sec",
            "range": "stddev: 0.0003844861915503716",
            "extra": "mean: 71.29901913333279 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.849838705883447,
            "unit": "iter/sec",
            "range": "stddev: 0.0009019724750374441",
            "extra": "mean: 350.89705179998987 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1233.5824193045596,
            "unit": "iter/sec",
            "range": "stddev: 0.00025411465290053",
            "extra": "mean: 810.6470912286159 usec\nrounds: 855"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 770.3899157211781,
            "unit": "iter/sec",
            "range": "stddev: 0.0005491629839001864",
            "extra": "mean: 1.298043989924088 msec\nrounds: 794"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.0604512705299274,
            "unit": "iter/sec",
            "range": "stddev: 0.022437031303124497",
            "extra": "mean: 326.7491985999982 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6637435944805488,
            "unit": "iter/sec",
            "range": "stddev: 0.03851028472763033",
            "extra": "mean: 1.506605876599997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1239.3940501307052,
            "unit": "iter/sec",
            "range": "stddev: 0.00012595385358833496",
            "extra": "mean: 806.8458936804974 usec\nrounds: 997"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 415.76335910900474,
            "unit": "iter/sec",
            "range": "stddev: 0.0004221932473110016",
            "extra": "mean: 2.4052143559332273 msec\nrounds: 236"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8044002514918722,
            "unit": "iter/sec",
            "range": "stddev: 0.016573890281068246",
            "extra": "mean: 1.2431622170000083 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16078467243584227,
            "unit": "iter/sec",
            "range": "stddev: 0.09146712355789573",
            "extra": "mean: 6.219498319399998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1248.1917749460783,
            "unit": "iter/sec",
            "range": "stddev: 0.00048218241041831283",
            "extra": "mean: 801.1589405347586 usec\nrounds: 1009"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 759.2830198525493,
            "unit": "iter/sec",
            "range": "stddev: 0.0007346964069864696",
            "extra": "mean: 1.3170319549542902 msec\nrounds: 777"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.9769050940313355,
            "unit": "iter/sec",
            "range": "stddev: 0.00863918077379835",
            "extra": "mean: 251.451813999995 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8082758783936785,
            "unit": "iter/sec",
            "range": "stddev: 0.011136803291270474",
            "extra": "mean: 1.2372013402000106 sec\nrounds: 5"
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
          "id": "f12d5f605d3915c1f4595903cfe37b37f6bc921b",
          "message": "Add Extend step. (#66)\n\n* Add Extend step.\r\n\r\n* Fix duplicate imports.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-22T21:51:57+02:00",
          "tree_id": "5557d18a52af68453e20b2d3f60d242323968b86",
          "url": "https://github.com/mlrun/storey/commit/f12d5f605d3915c1f4595903cfe37b37f6bc921b"
        },
        "date": 1606074960885,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1179.3753512989742,
            "unit": "iter/sec",
            "range": "stddev: 0.000417620203193488",
            "extra": "mean: 847.9064776948164 usec\nrounds: 538"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 769.9407506498071,
            "unit": "iter/sec",
            "range": "stddev: 0.00013262020747936048",
            "extra": "mean: 1.298801237830352 msec\nrounds: 719"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.004182253698611,
            "unit": "iter/sec",
            "range": "stddev: 0.01916443901044186",
            "extra": "mean: 332.86928539999394 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.5989442209690461,
            "unit": "iter/sec",
            "range": "stddev: 0.03248326690530171",
            "extra": "mean: 1.6696045557999981 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3573.9478433668246,
            "unit": "iter/sec",
            "range": "stddev: 0.00004748820848495844",
            "extra": "mean: 279.8026283052731 usec\nrounds: 2685"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2468.8518903599197,
            "unit": "iter/sec",
            "range": "stddev: 0.00006078484084366093",
            "extra": "mean: 405.0465740390023 usec\nrounds: 1587"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 11.315551378105802,
            "unit": "iter/sec",
            "range": "stddev: 0.0013160322861176567",
            "extra": "mean: 88.3739524999972 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.267892110254444,
            "unit": "iter/sec",
            "range": "stddev: 0.0052251971913090224",
            "extra": "mean: 440.9380832000011 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1156.5544759806762,
            "unit": "iter/sec",
            "range": "stddev: 0.00046714184697181383",
            "extra": "mean: 864.6371794567402 usec\nrounds: 847"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 749.2852725278746,
            "unit": "iter/sec",
            "range": "stddev: 0.00014424475209741164",
            "extra": "mean: 1.3346051719744678 msec\nrounds: 785"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.6273751982159537,
            "unit": "iter/sec",
            "range": "stddev: 0.019273510940364474",
            "extra": "mean: 380.60799259999953 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5387489645816002,
            "unit": "iter/sec",
            "range": "stddev: 0.055215516953104185",
            "extra": "mean: 1.856152059199991 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1105.377905192617,
            "unit": "iter/sec",
            "range": "stddev: 0.00021133442237148445",
            "extra": "mean: 904.6679830512313 usec\nrounds: 885"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 420.41491481238967,
            "unit": "iter/sec",
            "range": "stddev: 0.00021684481546514154",
            "extra": "mean: 2.3786025775185693 msec\nrounds: 258"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8218777123407932,
            "unit": "iter/sec",
            "range": "stddev: 0.017025954322660106",
            "extra": "mean: 1.2167260225999996 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16358563070157886,
            "unit": "iter/sec",
            "range": "stddev: 0.037666989735385174",
            "extra": "mean: 6.113006354599998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1100.2299764937677,
            "unit": "iter/sec",
            "range": "stddev: 0.0005879609712722421",
            "extra": "mean: 908.9008856010428 usec\nrounds: 1014"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 700.8510465628052,
            "unit": "iter/sec",
            "range": "stddev: 0.0004378549777486478",
            "extra": "mean: 1.426836707891521 msec\nrounds: 849"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.03166162995906,
            "unit": "iter/sec",
            "range": "stddev: 0.046308869667344287",
            "extra": "mean: 329.85211479999634 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6343672174711671,
            "unit": "iter/sec",
            "range": "stddev: 0.02625806143247335",
            "extra": "mean: 1.5763740188000042 sec\nrounds: 5"
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
          "id": "c831671851416aa728ec5aba3d0702eabb87623b",
          "message": "Avoid dropping metadata columns in DataframeSource. (#68)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-23T16:53:07+02:00",
          "tree_id": "d4decb2ab9fb9290152f9ae0db4dc62543997ae2",
          "url": "https://github.com/mlrun/storey/commit/c831671851416aa728ec5aba3d0702eabb87623b"
        },
        "date": 1606143433144,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1214.1389240926644,
            "unit": "iter/sec",
            "range": "stddev: 0.00016156685661769882",
            "extra": "mean: 823.6289770112656 usec\nrounds: 522"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 818.1881487088774,
            "unit": "iter/sec",
            "range": "stddev: 0.00012931224208486607",
            "extra": "mean: 1.22221276558213 msec\nrounds: 738"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 3.9053603772126104,
            "unit": "iter/sec",
            "range": "stddev: 0.004112653851927308",
            "extra": "mean: 256.0583155999893 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7822123978347569,
            "unit": "iter/sec",
            "range": "stddev: 0.021466331543916445",
            "extra": "mean: 1.2784251473999917 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4650.852819275756,
            "unit": "iter/sec",
            "range": "stddev: 0.000011523870898561596",
            "extra": "mean: 215.0143293839436 usec\nrounds: 2110"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3092.3801594776933,
            "unit": "iter/sec",
            "range": "stddev: 0.000013822950320887291",
            "extra": "mean: 323.3755063830513 usec\nrounds: 1645"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 13.878689731697529,
            "unit": "iter/sec",
            "range": "stddev: 0.0010842538002981232",
            "extra": "mean: 72.05291128571746 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 2.803564708977497,
            "unit": "iter/sec",
            "range": "stddev: 0.0006924101442235686",
            "extra": "mean: 356.6887530000031 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1246.8765269504108,
            "unit": "iter/sec",
            "range": "stddev: 0.00009729521709638955",
            "extra": "mean: 802.0040303796422 usec\nrounds: 790"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 780.8685038637582,
            "unit": "iter/sec",
            "range": "stddev: 0.00006482940295724281",
            "extra": "mean: 1.2806253486367722 msec\nrounds: 697"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.3878616739086254,
            "unit": "iter/sec",
            "range": "stddev: 0.007000988765302564",
            "extra": "mean: 295.1714374000062 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6862580921555625,
            "unit": "iter/sec",
            "range": "stddev: 0.05088407045434905",
            "extra": "mean: 1.4571777169999733 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1224.2929358941333,
            "unit": "iter/sec",
            "range": "stddev: 0.00008167753436403185",
            "extra": "mean: 816.7979824776771 usec\nrounds: 856"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 426.98314540308417,
            "unit": "iter/sec",
            "range": "stddev: 0.0002813784091532418",
            "extra": "mean: 2.342012818927482 msec\nrounds: 243"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8738471492726033,
            "unit": "iter/sec",
            "range": "stddev: 0.019069331604077273",
            "extra": "mean: 1.144364893599993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1724068093851505,
            "unit": "iter/sec",
            "range": "stddev: 0.06905332923919115",
            "extra": "mean: 5.800234941799988 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1248.4177956466867,
            "unit": "iter/sec",
            "range": "stddev: 0.00009358508349846524",
            "extra": "mean: 801.0138941362935 usec\nrounds: 614"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 812.6477427094864,
            "unit": "iter/sec",
            "range": "stddev: 0.0000771453411567538",
            "extra": "mean: 1.2305454718496527 msec\nrounds: 746"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.159434581503468,
            "unit": "iter/sec",
            "range": "stddev: 0.0071204949055710485",
            "extra": "mean: 240.41729240000222 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8252509536819512,
            "unit": "iter/sec",
            "range": "stddev: 0.0447410246787712",
            "extra": "mean: 1.2117526136000039 sec\nrounds: 5"
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
          "id": "4431683a9ba885f5f7e87386acd15ac702e2cc67",
          "message": "Make test faster and more reliable. (#67)\n\n* Make test faster and more reliable.\r\n\r\n* Minor refactoring.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-23T17:09:53+02:00",
          "tree_id": "a523fa73e5f65754beec138660b2bed4fe2b9005",
          "url": "https://github.com/mlrun/storey/commit/4431683a9ba885f5f7e87386acd15ac702e2cc67"
        },
        "date": 1606144426979,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1352.6512821089666,
            "unit": "iter/sec",
            "range": "stddev: 0.00010692409841333652",
            "extra": "mean: 739.2888420146725 usec\nrounds: 576"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 907.948206379494,
            "unit": "iter/sec",
            "range": "stddev: 0.00009598584232834413",
            "extra": "mean: 1.1013844104473414 msec\nrounds: 938"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.167717633076061,
            "unit": "iter/sec",
            "range": "stddev: 0.00985856017947172",
            "extra": "mean: 239.93947959999673 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.8512490846261068,
            "unit": "iter/sec",
            "range": "stddev: 0.06315591135569053",
            "extra": "mean: 1.174744288200003 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4957.875111063017,
            "unit": "iter/sec",
            "range": "stddev: 0.00002245285822009004",
            "extra": "mean: 201.69931222523073 usec\nrounds: 2274"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3476.432744861782,
            "unit": "iter/sec",
            "range": "stddev: 0.000052410518151698497",
            "extra": "mean: 287.65118539342217 usec\nrounds: 2492"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 14.801796207443363,
            "unit": "iter/sec",
            "range": "stddev: 0.003647040413489095",
            "extra": "mean: 67.55936819999799 msec\nrounds: 15"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 3.006755271053912,
            "unit": "iter/sec",
            "range": "stddev: 0.015789948325281116",
            "extra": "mean: 332.584434000006 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1362.497939050858,
            "unit": "iter/sec",
            "range": "stddev: 0.00006431307623582397",
            "extra": "mean: 733.946064312302 usec\nrounds: 793"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 828.0498137935213,
            "unit": "iter/sec",
            "range": "stddev: 0.00032571505487539526",
            "extra": "mean: 1.2076568140493 msec\nrounds: 726"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.7709665087393933,
            "unit": "iter/sec",
            "range": "stddev: 0.011688886349340313",
            "extra": "mean: 265.1840046000018 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7821282989167515,
            "unit": "iter/sec",
            "range": "stddev: 0.02820275778294809",
            "extra": "mean: 1.2785626110000123 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1340.572060188182,
            "unit": "iter/sec",
            "range": "stddev: 0.00006616844788683426",
            "extra": "mean: 745.9502026766287 usec\nrounds: 1046"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 475.8393143846026,
            "unit": "iter/sec",
            "range": "stddev: 0.0014526844940052433",
            "extra": "mean: 2.1015497664233322 msec\nrounds: 274"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9692637074568893,
            "unit": "iter/sec",
            "range": "stddev: 0.02520263829541737",
            "extra": "mean: 1.0317109702000038 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.18586706020758306,
            "unit": "iter/sec",
            "range": "stddev: 0.06893286051270067",
            "extra": "mean: 5.380189469199996 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1399.8488354394337,
            "unit": "iter/sec",
            "range": "stddev: 0.00006659103341517597",
            "extra": "mean: 714.3628473899361 usec\nrounds: 996"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 873.5885495981125,
            "unit": "iter/sec",
            "range": "stddev: 0.00013907304868062328",
            "extra": "mean: 1.1447036484853677 msec\nrounds: 825"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.6619019697583415,
            "unit": "iter/sec",
            "range": "stddev: 0.00911512230488287",
            "extra": "mean: 214.50472500000615 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9208048228403346,
            "unit": "iter/sec",
            "range": "stddev: 0.017816550012420773",
            "extra": "mean: 1.0860064752000085 sec\nrounds: 5"
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
          "id": "7ea80b0cafa16a66bf2c0d3e7eb67dc96c96dfca",
          "message": "Optimizations and error propagation fix. (#71)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-23T17:34:51+02:00",
          "tree_id": "4533ac97bb1ef33f9c7c2a57b7c416ad7cb317fe",
          "url": "https://github.com/mlrun/storey/commit/7ea80b0cafa16a66bf2c0d3e7eb67dc96c96dfca"
        },
        "date": 1606145916891,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1285.0082574365088,
            "unit": "iter/sec",
            "range": "stddev: 0.000043189366048665015",
            "extra": "mean: 778.2051159693884 usec\nrounds: 526"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 909.2517833203062,
            "unit": "iter/sec",
            "range": "stddev: 0.00005842104779032282",
            "extra": "mean: 1.0998053766233038 msec\nrounds: 616"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.5570209123444165,
            "unit": "iter/sec",
            "range": "stddev: 0.0029340784792695846",
            "extra": "mean: 152.50828285713933 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.1162848280941784,
            "unit": "iter/sec",
            "range": "stddev: 0.015348348130597036",
            "extra": "mean: 895.8287121999945 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4698.703417929871,
            "unit": "iter/sec",
            "range": "stddev: 0.000029843166375477667",
            "extra": "mean: 212.82466907446874 usec\nrounds: 2215"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3956.883799168925,
            "unit": "iter/sec",
            "range": "stddev: 0.00005442432959956181",
            "extra": "mean: 252.72412604333556 usec\nrounds: 2277"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 57.87355326821728,
            "unit": "iter/sec",
            "range": "stddev: 0.0005740465267400766",
            "extra": "mean: 17.279049644065577 msec\nrounds: 59"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.506939329283862,
            "unit": "iter/sec",
            "range": "stddev: 0.008523986885106543",
            "extra": "mean: 86.90408208333149 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1322.3885295008333,
            "unit": "iter/sec",
            "range": "stddev: 0.0000511890075818938",
            "extra": "mean: 756.2074062888865 usec\nrounds: 795"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 847.1410416690334,
            "unit": "iter/sec",
            "range": "stddev: 0.00036422288455825914",
            "extra": "mean: 1.180440978316674 msec\nrounds: 784"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.4701946051032735,
            "unit": "iter/sec",
            "range": "stddev: 0.013569623619560425",
            "extra": "mean: 223.70390739999948 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9388449271027632,
            "unit": "iter/sec",
            "range": "stddev: 0.018599478290616936",
            "extra": "mean: 1.0651386306000064 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1253.6417811046338,
            "unit": "iter/sec",
            "range": "stddev: 0.00005609855394993857",
            "extra": "mean: 797.6760308027226 usec\nrounds: 909"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 449.0530850615929,
            "unit": "iter/sec",
            "range": "stddev: 0.00008933482190615014",
            "extra": "mean: 2.2269082058813563 msec\nrounds: 340"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.0041845555367028,
            "unit": "iter/sec",
            "range": "stddev: 0.020306397561326975",
            "extra": "mean: 995.8328819999963 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.19938965738006026,
            "unit": "iter/sec",
            "range": "stddev: 0.10464775999162347",
            "extra": "mean: 5.015305272799992 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1334.337947460458,
            "unit": "iter/sec",
            "range": "stddev: 0.0000768204879848458",
            "extra": "mean: 749.435330010079 usec\nrounds: 1003"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 903.1618913752294,
            "unit": "iter/sec",
            "range": "stddev: 0.00017388825204548496",
            "extra": "mean: 1.107221207570347 msec\nrounds: 819"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.053499435707802,
            "unit": "iter/sec",
            "range": "stddev: 0.005402844865011267",
            "extra": "mean: 165.19370500000312 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0272705787437768,
            "unit": "iter/sec",
            "range": "stddev: 0.029235473847585434",
            "extra": "mean: 973.4533634000059 msec\nrounds: 5"
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
          "id": "67229b28fa5494890233e5379249cedffeed9d04",
          "message": "fix crash when querying for aggregates with a smaller window then ingested (#70)",
          "timestamp": "2020-11-23T17:41:53+02:00",
          "tree_id": "40b370ca724c94e33cb308d08bdfd156fa4fa656",
          "url": "https://github.com/mlrun/storey/commit/67229b28fa5494890233e5379249cedffeed9d04"
        },
        "date": 1606146347103,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1151.6992672264264,
            "unit": "iter/sec",
            "range": "stddev: 0.00019184352698098167",
            "extra": "mean: 868.2822230218524 usec\nrounds: 556"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 779.9420474266708,
            "unit": "iter/sec",
            "range": "stddev: 0.00038965280177644465",
            "extra": "mean: 1.2821465431943118 msec\nrounds: 764"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.011956166782346,
            "unit": "iter/sec",
            "range": "stddev: 0.008516818199161953",
            "extra": "mean: 199.52289420000966 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7667228515754135,
            "unit": "iter/sec",
            "range": "stddev: 0.020854822106855186",
            "extra": "mean: 1.3042522443999984 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3553.783465435218,
            "unit": "iter/sec",
            "range": "stddev: 0.00009362056424993641",
            "extra": "mean: 281.3902449955639 usec\nrounds: 2098"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2866.5942658019585,
            "unit": "iter/sec",
            "range": "stddev: 0.00010272382496751647",
            "extra": "mean: 348.84601979772674 usec\nrounds: 2374"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 51.57462045888002,
            "unit": "iter/sec",
            "range": "stddev: 0.0010777784063910184",
            "extra": "mean: 19.389381659091242 msec\nrounds: 44"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.495296544660777,
            "unit": "iter/sec",
            "range": "stddev: 0.006540449579240924",
            "extra": "mean: 95.28077608333281 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1125.2287862117043,
            "unit": "iter/sec",
            "range": "stddev: 0.00045384231304624375",
            "extra": "mean: 888.7081562912101 usec\nrounds: 755"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 756.3773306956186,
            "unit": "iter/sec",
            "range": "stddev: 0.0003690771769248515",
            "extra": "mean: 1.3220914475058745 msec\nrounds: 762"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.435276734629451,
            "unit": "iter/sec",
            "range": "stddev: 0.009502411721808664",
            "extra": "mean: 291.09736340000154 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6954946350646177,
            "unit": "iter/sec",
            "range": "stddev: 0.024393140343368647",
            "extra": "mean: 1.4378256130000069 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1097.5802974132462,
            "unit": "iter/sec",
            "range": "stddev: 0.0003043080692558844",
            "extra": "mean: 911.0950719111655 usec\nrounds: 890"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 429.79486772111903,
            "unit": "iter/sec",
            "range": "stddev: 0.00029661961087966215",
            "extra": "mean: 2.3266913476706983 msec\nrounds: 279"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8904399361616044,
            "unit": "iter/sec",
            "range": "stddev: 0.026729872338161424",
            "extra": "mean: 1.1230403752000087 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1658581763078057,
            "unit": "iter/sec",
            "range": "stddev: 0.12713064658488543",
            "extra": "mean: 6.029247530999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1138.5013507839224,
            "unit": "iter/sec",
            "range": "stddev: 0.00031946767499463813",
            "extra": "mean: 878.3476623119011 usec\nrounds: 995"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 725.448342313832,
            "unit": "iter/sec",
            "range": "stddev: 0.0008567595633412406",
            "extra": "mean: 1.3784579020616134 msec\nrounds: 776"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.722760088447815,
            "unit": "iter/sec",
            "range": "stddev: 0.00495203025907963",
            "extra": "mean: 211.74058840000498 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7432272183002383,
            "unit": "iter/sec",
            "range": "stddev: 0.01821914826749673",
            "extra": "mean: 1.3454835551999849 sec\nrounds: 5"
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
          "id": "6e5d569ef261d3295a8e0dccb456a4f7e516a7e1",
          "message": "Parallelize downstream processing when there are 2 or more outlets. (#72)\n\n* Parallelize downstream processing when there are 2 or more outlets.\r\n\r\n* Added comments.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-24T17:22:03+02:00",
          "tree_id": "69aabc95533e30fe994f60ddec8e9b6ffd5a0da1",
          "url": "https://github.com/mlrun/storey/commit/6e5d569ef261d3295a8e0dccb456a4f7e516a7e1"
        },
        "date": 1606231577240,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1070.7154492215925,
            "unit": "iter/sec",
            "range": "stddev: 0.0007277619091940869",
            "extra": "mean: 933.9549557513133 usec\nrounds: 565"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 754.6471167130106,
            "unit": "iter/sec",
            "range": "stddev: 0.0006925369418413224",
            "extra": "mean: 1.3251226670760554 msec\nrounds: 814"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.214487415800105,
            "unit": "iter/sec",
            "range": "stddev: 0.0290720951152209",
            "extra": "mean: 237.27677920000474 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6710957731402643,
            "unit": "iter/sec",
            "range": "stddev: 0.06811191379241036",
            "extra": "mean: 1.4901002807999988 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3355.6833754517097,
            "unit": "iter/sec",
            "range": "stddev: 0.00008829926475541962",
            "extra": "mean: 298.00189353841813 usec\nrounds: 1888"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2791.9398479953175,
            "unit": "iter/sec",
            "range": "stddev: 0.00010129964249957773",
            "extra": "mean: 358.1739057587595 usec\nrounds: 2292"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 44.037713352874746,
            "unit": "iter/sec",
            "range": "stddev: 0.002540995503457748",
            "extra": "mean: 22.707809372094044 msec\nrounds: 43"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.026700479940573,
            "unit": "iter/sec",
            "range": "stddev: 0.010688467856566033",
            "extra": "mean: 110.78245059999858 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1126.194375282812,
            "unit": "iter/sec",
            "range": "stddev: 0.00032692186408103896",
            "extra": "mean: 887.9461857984135 usec\nrounds: 845"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 730.7393872601951,
            "unit": "iter/sec",
            "range": "stddev: 0.0007059996408564723",
            "extra": "mean: 1.368476939157967 msec\nrounds: 641"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.0107111577100594,
            "unit": "iter/sec",
            "range": "stddev: 0.01819799616137813",
            "extra": "mean: 332.14743880000697 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5965253759078708,
            "unit": "iter/sec",
            "range": "stddev: 0.13790578391362543",
            "extra": "mean: 1.676374619400002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1058.1142256531077,
            "unit": "iter/sec",
            "range": "stddev: 0.0005189542599587767",
            "extra": "mean: 945.077549999635 usec\nrounds: 360"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 363.9393631096955,
            "unit": "iter/sec",
            "range": "stddev: 0.001296326497396444",
            "extra": "mean: 2.7477104742269622 msec\nrounds: 388"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.817524368651923,
            "unit": "iter/sec",
            "range": "stddev: 0.043096291828433365",
            "extra": "mean: 1.2232051280000065 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15044092402612047,
            "unit": "iter/sec",
            "range": "stddev: 0.08365653264272886",
            "extra": "mean: 6.647127478599998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 951.1234187498985,
            "unit": "iter/sec",
            "range": "stddev: 0.0010384003428228318",
            "extra": "mean: 1.0513882639062153 msec\nrounds: 773"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 547.8994424197502,
            "unit": "iter/sec",
            "range": "stddev: 0.002024607053494312",
            "extra": "mean: 1.8251524323214983 msec\nrounds: 495"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.650493483346285,
            "unit": "iter/sec",
            "range": "stddev: 0.02398990728458902",
            "extra": "mean: 273.93556640000725 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6375816677911965,
            "unit": "iter/sec",
            "range": "stddev: 0.04301958969021389",
            "extra": "mean: 1.5684265256000003 sec\nrounds: 5"
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
          "id": "fc475e43b3189c34f45c73ca87348227ee5a120e",
          "message": "Query by key (#69)\n\n* queryByKey\r\n\r\n* revert test\r\n\r\n* flip case\r\n\r\n* code review\r\n\r\n* return avg to tets\r\n\r\n* remove type in doc string\r\n\r\n* pass use_windows_from_schema to aggr_store\r\n\r\nCo-authored-by: Dina Nimrodi <dinan@iguazio.com>",
          "timestamp": "2020-11-26T10:30:06+02:00",
          "tree_id": "1d321b09643bc069ffb6d4ee31885f86dc3c0b08",
          "url": "https://github.com/mlrun/storey/commit/fc475e43b3189c34f45c73ca87348227ee5a120e"
        },
        "date": 1606379621950,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1514.1259712850265,
            "unit": "iter/sec",
            "range": "stddev: 0.00008096414489370098",
            "extra": "mean: 660.4470294841506 usec\nrounds: 407"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 1020.6857382123358,
            "unit": "iter/sec",
            "range": "stddev: 0.0002843791488788798",
            "extra": "mean: 979.733489517973 usec\nrounds: 954"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.498986737697261,
            "unit": "iter/sec",
            "range": "stddev: 0.007379962209987989",
            "extra": "mean: 153.8701401250009 msec\nrounds: 8"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9670990901185916,
            "unit": "iter/sec",
            "range": "stddev: 0.06576111941220429",
            "extra": "mean: 1.034020205600001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5439.818194573766,
            "unit": "iter/sec",
            "range": "stddev: 0.000013448979253060414",
            "extra": "mean: 183.82967302059888 usec\nrounds: 2413"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4335.241019077953,
            "unit": "iter/sec",
            "range": "stddev: 0.00004747288097770117",
            "extra": "mean: 230.66768274228187 usec\nrounds: 2509"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 60.30428224347044,
            "unit": "iter/sec",
            "range": "stddev: 0.00008359030347621739",
            "extra": "mean: 16.582570305084374 msec\nrounds: 59"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.868368010733386,
            "unit": "iter/sec",
            "range": "stddev: 0.008475588249400364",
            "extra": "mean: 84.25758276922579 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1407.5078875776182,
            "unit": "iter/sec",
            "range": "stddev: 0.00009735817826084292",
            "extra": "mean: 710.4755922334781 usec\nrounds: 824"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 943.629371365083,
            "unit": "iter/sec",
            "range": "stddev: 0.00021328892795467603",
            "extra": "mean: 1.059738103057739 msec\nrounds: 883"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.28267429544112,
            "unit": "iter/sec",
            "range": "stddev: 0.0270472697142163",
            "extra": "mean: 233.4989614000051 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9125529528151314,
            "unit": "iter/sec",
            "range": "stddev: 0.08361435901967944",
            "extra": "mean: 1.0958268196000063 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1372.2868450881429,
            "unit": "iter/sec",
            "range": "stddev: 0.0003563719032125811",
            "extra": "mean: 728.7106216745591 usec\nrounds: 1015"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 463.6993521075871,
            "unit": "iter/sec",
            "range": "stddev: 0.0015189948938945227",
            "extra": "mean: 2.15656975895015 msec\nrounds: 419"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.1394151686072331,
            "unit": "iter/sec",
            "range": "stddev: 0.02565158608749227",
            "extra": "mean: 877.6432221999926 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.20056158180732,
            "unit": "iter/sec",
            "range": "stddev: 0.09928534434170036",
            "extra": "mean: 4.985999766200001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1340.0261714514752,
            "unit": "iter/sec",
            "range": "stddev: 0.00038134944379320006",
            "extra": "mean: 746.2540816772487 usec\nrounds: 906"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 853.6757474118087,
            "unit": "iter/sec",
            "range": "stddev: 0.0006207338862485244",
            "extra": "mean: 1.1714049544359437 msec\nrounds: 834"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.26554766282768,
            "unit": "iter/sec",
            "range": "stddev: 0.012150123935202668",
            "extra": "mean: 159.60296750000205 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0402241081453825,
            "unit": "iter/sec",
            "range": "stddev: 0.059245766804582906",
            "extra": "mean: 961.3313056000038 msec\nrounds: 5"
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
          "id": "87283b13e500a9ff8bcc01e741664e55bf3905fa",
          "message": "Context object + minor refactoring (#75)\n\n* create Context, and use it in related steps + minor refactoring\r\n\r\n* refarctoring + change comments+typing",
          "timestamp": "2020-11-26T12:11:00+02:00",
          "tree_id": "aa0903d939f5d4741e37650d998f171d4e3c09fe",
          "url": "https://github.com/mlrun/storey/commit/87283b13e500a9ff8bcc01e741664e55bf3905fa"
        },
        "date": 1606385689502,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1337.3097179650226,
            "unit": "iter/sec",
            "range": "stddev: 0.0002824986401669944",
            "extra": "mean: 747.7699343437772 usec\nrounds: 396"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 938.7204565001085,
            "unit": "iter/sec",
            "range": "stddev: 0.00027207654245966855",
            "extra": "mean: 1.0652798637502414 msec\nrounds: 800"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 7.013512035909663,
            "unit": "iter/sec",
            "range": "stddev: 0.0015432761998533898",
            "extra": "mean: 142.5819182857221 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.1540687402381626,
            "unit": "iter/sec",
            "range": "stddev: 0.0097894443062324",
            "extra": "mean: 866.4995117999922 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5449.232007850304,
            "unit": "iter/sec",
            "range": "stddev: 0.000025906828884001416",
            "extra": "mean: 183.5120983212633 usec\nrounds: 2085"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4251.400963983667,
            "unit": "iter/sec",
            "range": "stddev: 0.000053311041551157654",
            "extra": "mean: 235.21658118621104 usec\nrounds: 2445"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 61.411193790270495,
            "unit": "iter/sec",
            "range": "stddev: 0.0008152726392154381",
            "extra": "mean: 16.283676285713764 msec\nrounds: 56"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.83719679428641,
            "unit": "iter/sec",
            "range": "stddev: 0.011667996289524998",
            "extra": "mean: 84.47946058332671 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1335.9104982688018,
            "unit": "iter/sec",
            "range": "stddev: 0.0000731876418667388",
            "extra": "mean: 748.5531413188935 usec\nrounds: 743"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 876.279560182547,
            "unit": "iter/sec",
            "range": "stddev: 0.00009052941834387654",
            "extra": "mean: 1.14118832098706 msec\nrounds: 891"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.897441002431981,
            "unit": "iter/sec",
            "range": "stddev: 0.006100448592081939",
            "extra": "mean: 204.18826883333927 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 1.0078862070380041,
            "unit": "iter/sec",
            "range": "stddev: 0.01996469717434086",
            "extra": "mean: 992.1754986000053 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1337.9597510447772,
            "unit": "iter/sec",
            "range": "stddev: 0.0000771621250503199",
            "extra": "mean: 747.4066385174341 usec\nrounds: 971"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 472.6495009374822,
            "unit": "iter/sec",
            "range": "stddev: 0.001236121454684828",
            "extra": "mean: 2.1157326899034876 msec\nrounds: 416"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.03368717088146,
            "unit": "iter/sec",
            "range": "stddev: 0.011094699504261745",
            "extra": "mean: 967.4106714000004 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.19031748012564328,
            "unit": "iter/sec",
            "range": "stddev: 0.07255356700367534",
            "extra": "mean: 5.254378101999999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1315.3363965701992,
            "unit": "iter/sec",
            "range": "stddev: 0.00007400776908358894",
            "extra": "mean: 760.2617874845907 usec\nrounds: 767"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 895.3750479316149,
            "unit": "iter/sec",
            "range": "stddev: 0.00010052774379429735",
            "extra": "mean: 1.116850421854034 msec\nrounds: 787"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.146674068644669,
            "unit": "iter/sec",
            "range": "stddev: 0.0027927428446832157",
            "extra": "mean: 162.68960885711945 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.087135933672959,
            "unit": "iter/sec",
            "range": "stddev: 0.02282254200921071",
            "extra": "mean: 919.8481707999804 msec\nrounds: 5"
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
          "id": "4be86060df9ad7d62d0da60940a05ce5ef1a36b4",
          "message": "Update dependencies, fix all warnings. (#76)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-26T12:14:35+02:00",
          "tree_id": "28c7b497d82b295552be3e39143c1c025a0af8c9",
          "url": "https://github.com/mlrun/storey/commit/4be86060df9ad7d62d0da60940a05ce5ef1a36b4"
        },
        "date": 1606385911496,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1559.4777998063214,
            "unit": "iter/sec",
            "range": "stddev: 0.00007201591758470094",
            "extra": "mean: 641.2402921825462 usec\nrounds: 486"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 1090.3955751949281,
            "unit": "iter/sec",
            "range": "stddev: 0.0001015604413671932",
            "extra": "mean: 917.0983657203778 usec\nrounds: 916"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 7.725780930772988,
            "unit": "iter/sec",
            "range": "stddev: 0.003602276994493406",
            "extra": "mean: 129.4367532499976 msec\nrounds: 8"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.1854857648022867,
            "unit": "iter/sec",
            "range": "stddev: 0.012670932923149029",
            "extra": "mean: 843.536067399998 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5952.950610397986,
            "unit": "iter/sec",
            "range": "stddev: 0.000024832478137244392",
            "extra": "mean: 167.98392351068821 usec\nrounds: 2484"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4693.212156658081,
            "unit": "iter/sec",
            "range": "stddev: 0.00004556163566650013",
            "extra": "mean: 213.07368314499445 usec\nrounds: 3358"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 64.04122807953446,
            "unit": "iter/sec",
            "range": "stddev: 0.0031646399201897884",
            "extra": "mean: 15.61494103076965 msec\nrounds: 65"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 13.214735873926491,
            "unit": "iter/sec",
            "range": "stddev: 0.004509447863443561",
            "extra": "mean: 75.67309778571233 msec\nrounds: 14"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1514.4922309766205,
            "unit": "iter/sec",
            "range": "stddev: 0.0001430901410518631",
            "extra": "mean: 660.2873092027352 usec\nrounds: 815"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 999.1676265297008,
            "unit": "iter/sec",
            "range": "stddev: 0.00010624387577621907",
            "extra": "mean: 1.00083306689308 msec\nrounds: 882"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 5.594518293303293,
            "unit": "iter/sec",
            "range": "stddev: 0.005698592901386681",
            "extra": "mean: 178.74639916666504 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 1.136103390967301,
            "unit": "iter/sec",
            "range": "stddev: 0.021491313676594046",
            "extra": "mean: 880.2015802000028 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1483.663118369346,
            "unit": "iter/sec",
            "range": "stddev: 0.00032698381576005286",
            "extra": "mean: 674.0074533220675 usec\nrounds: 1189"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 512.0171558723223,
            "unit": "iter/sec",
            "range": "stddev: 0.0011827167784392999",
            "extra": "mean: 1.9530595577335736 msec\nrounds: 459"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.1854740395810557,
            "unit": "iter/sec",
            "range": "stddev: 0.020624284002257902",
            "extra": "mean: 843.5444105999977 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.2208545633378592,
            "unit": "iter/sec",
            "range": "stddev: 0.04719735199555892",
            "extra": "mean: 4.527866596399997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1531.5240169719668,
            "unit": "iter/sec",
            "range": "stddev: 0.00007690053808168862",
            "extra": "mean: 652.9443801848679 usec\nrounds: 868"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 1054.6732982349736,
            "unit": "iter/sec",
            "range": "stddev: 0.00010526095044734705",
            "extra": "mean: 948.1609154925312 usec\nrounds: 923"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 7.213400274333752,
            "unit": "iter/sec",
            "range": "stddev: 0.0018144134331684286",
            "extra": "mean: 138.6308761428552 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.1076425022779477,
            "unit": "iter/sec",
            "range": "stddev: 0.011764098179366795",
            "extra": "mean: 902.8183714000022 msec\nrounds: 5"
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
          "id": "19ad46b4118dfddc0d20b765940223a840f81df2",
          "message": "Force flush to CSV. (#77)\n\n* Force flush to CSV.\r\n\r\n* Fix.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-26T15:41:15+02:00",
          "tree_id": "974f09b064e916859e3fa4c4e57bf7ff31de712f",
          "url": "https://github.com/mlrun/storey/commit/19ad46b4118dfddc0d20b765940223a840f81df2"
        },
        "date": 1606398332365,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1159.3376940718097,
            "unit": "iter/sec",
            "range": "stddev: 0.000159284132923325",
            "extra": "mean: 862.5614478968712 usec\nrounds: 547"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 807.286446312173,
            "unit": "iter/sec",
            "range": "stddev: 0.00032749402109789086",
            "extra": "mean: 1.2387176875917791 msec\nrounds: 685"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.94822336588375,
            "unit": "iter/sec",
            "range": "stddev: 0.007335103382958189",
            "extra": "mean: 202.0927363333366 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7879171662065444,
            "unit": "iter/sec",
            "range": "stddev: 0.03249668325379502",
            "extra": "mean: 1.2691689467999994 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3774.188301245546,
            "unit": "iter/sec",
            "range": "stddev: 0.00003946471346275818",
            "extra": "mean: 264.95763331945653 usec\nrounds: 2389"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3059.172820673998,
            "unit": "iter/sec",
            "range": "stddev: 0.00006911933611610732",
            "extra": "mean: 326.8857493901504 usec\nrounds: 2458"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 49.613651151847215,
            "unit": "iter/sec",
            "range": "stddev: 0.0011167591347105692",
            "extra": "mean: 20.15574296153707 msec\nrounds: 52"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.591101602570369,
            "unit": "iter/sec",
            "range": "stddev: 0.0015971760877581636",
            "extra": "mean: 94.41888460000314 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1163.5016343579327,
            "unit": "iter/sec",
            "range": "stddev: 0.0002530386382340824",
            "extra": "mean: 859.4745125148368 usec\nrounds: 839"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 742.5109597818283,
            "unit": "iter/sec",
            "range": "stddev: 0.0007863717689556397",
            "extra": "mean: 1.3467814674329244 msec\nrounds: 783"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.5025484199052603,
            "unit": "iter/sec",
            "range": "stddev: 0.009878092755752937",
            "extra": "mean: 285.5064028000072 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7065328830355296,
            "unit": "iter/sec",
            "range": "stddev: 0.013969652880956486",
            "extra": "mean: 1.4153622909999968 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1150.5967944544382,
            "unit": "iter/sec",
            "range": "stddev: 0.00011817722827871347",
            "extra": "mean: 869.1141891057985 usec\nrounds: 973"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 417.74399092941894,
            "unit": "iter/sec",
            "range": "stddev: 0.0012363929343231928",
            "extra": "mean: 2.393810615384669 msec\nrounds: 364"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8918397427488419,
            "unit": "iter/sec",
            "range": "stddev: 0.014392267349456075",
            "extra": "mean: 1.1212776825999982 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1701166677149477,
            "unit": "iter/sec",
            "range": "stddev: 0.05043067488319318",
            "extra": "mean: 5.878318764599999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1161.8175467920726,
            "unit": "iter/sec",
            "range": "stddev: 0.00014300550710280596",
            "extra": "mean: 860.720345256558 usec\nrounds: 643"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 731.880323622423,
            "unit": "iter/sec",
            "range": "stddev: 0.00017832554182910538",
            "extra": "mean: 1.3663436052639393 msec\nrounds: 760"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.674044342159531,
            "unit": "iter/sec",
            "range": "stddev: 0.002853594752699168",
            "extra": "mean: 213.94747819999793 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7218953114571782,
            "unit": "iter/sec",
            "range": "stddev: 0.015187175903101431",
            "extra": "mean: 1.3852424086000155 sec\nrounds: 5"
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
          "id": "bb9ddf4a5b85e87fb0dfc15983ec3b9ccb341aef",
          "message": "Fix CSV performance. (#78)\n\n* Fix CSV performance.\r\n\r\n* run_in_executor instead of creating a new thread.\r\n\r\n* Better configurability of WriteToCSV.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-11-29T13:24:46+02:00",
          "tree_id": "5e4aecd8ef9e92f5ba3a09287cf8178fbca56ece",
          "url": "https://github.com/mlrun/storey/commit/bb9ddf4a5b85e87fb0dfc15983ec3b9ccb341aef"
        },
        "date": 1606649302173,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1247.8169820036408,
            "unit": "iter/sec",
            "range": "stddev: 0.000401992697118041",
            "extra": "mean: 801.3995757569215 usec\nrounds: 660"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 919.537895021091,
            "unit": "iter/sec",
            "range": "stddev: 0.0001333270757527988",
            "extra": "mean: 1.0875027613484745 msec\nrounds: 771"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.029616961691588,
            "unit": "iter/sec",
            "range": "stddev: 0.0037862800587751292",
            "extra": "mean: 165.84801428571237 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.979266858082538,
            "unit": "iter/sec",
            "range": "stddev: 0.024010339148946232",
            "extra": "mean: 1.021172106199998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4876.116760795413,
            "unit": "iter/sec",
            "range": "stddev: 0.00001288544312541266",
            "extra": "mean: 205.08122529799218 usec\nrounds: 2348"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3884.8232390397716,
            "unit": "iter/sec",
            "range": "stddev: 0.00004947029939588781",
            "extra": "mean: 257.41196921154494 usec\nrounds: 2436"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 53.2232710365445,
            "unit": "iter/sec",
            "range": "stddev: 0.00019111310066775728",
            "extra": "mean: 18.78877379245206 msec\nrounds: 53"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.331551306350626,
            "unit": "iter/sec",
            "range": "stddev: 0.009661204099476108",
            "extra": "mean: 96.79088554545697 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1291.4883376175508,
            "unit": "iter/sec",
            "range": "stddev: 0.00014219508286580083",
            "extra": "mean: 774.3004492357488 usec\nrounds: 719"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 843.7211529619395,
            "unit": "iter/sec",
            "range": "stddev: 0.00008277505850072792",
            "extra": "mean: 1.1852257069642418 msec\nrounds: 761"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.341652166966659,
            "unit": "iter/sec",
            "range": "stddev: 0.008820689679065255",
            "extra": "mean: 230.3270648000023 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8708203052541637,
            "unit": "iter/sec",
            "range": "stddev: 0.046622516329376355",
            "extra": "mean: 1.148342538600008 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1178.1866738482156,
            "unit": "iter/sec",
            "range": "stddev: 0.0005461601758829703",
            "extra": "mean: 848.7619340777138 usec\nrounds: 986"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 450.9507630484941,
            "unit": "iter/sec",
            "range": "stddev: 0.000058829532176054406",
            "extra": "mean: 2.217536995036557 msec\nrounds: 403"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9018436510425014,
            "unit": "iter/sec",
            "range": "stddev: 0.035060843404516005",
            "extra": "mean: 1.108839651799991 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17273487103180638,
            "unit": "iter/sec",
            "range": "stddev: 0.09772582977702096",
            "extra": "mean: 5.789219015399999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1270.429652453173,
            "unit": "iter/sec",
            "range": "stddev: 0.00034856213671376044",
            "extra": "mean: 787.1352798393999 usec\nrounds: 997"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 888.3437756893413,
            "unit": "iter/sec",
            "range": "stddev: 0.00015785794422210368",
            "extra": "mean: 1.1256903322410463 msec\nrounds: 611"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.807742361161451,
            "unit": "iter/sec",
            "range": "stddev: 0.006670744546329529",
            "extra": "mean: 172.1839464999988 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9348946853907532,
            "unit": "iter/sec",
            "range": "stddev: 0.04229529921425289",
            "extra": "mean: 1.0696391964000043 sec\nrounds: 5"
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
          "id": "1f331f04d92178f5fd499303ce32fe9e0cba3837",
          "message": "Makedirs before write. (#79)\n\n* Makedirs before write.\r\n\r\n* Test makedirs in CSV writer also.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-01T13:48:22+02:00",
          "tree_id": "b2f3caaf8117bcf0b1dffc9b5cd30b3615f21a69",
          "url": "https://github.com/mlrun/storey/commit/1f331f04d92178f5fd499303ce32fe9e0cba3837"
        },
        "date": 1606823508132,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1367.610203739988,
            "unit": "iter/sec",
            "range": "stddev: 0.00014215553127457835",
            "extra": "mean: 731.2024999998622 usec\nrounds: 358"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 948.8641671479351,
            "unit": "iter/sec",
            "range": "stddev: 0.0002782830326358246",
            "extra": "mean: 1.0538916260330153 msec\nrounds: 968"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.677534965340164,
            "unit": "iter/sec",
            "range": "stddev: 0.005904732988466708",
            "extra": "mean: 149.755861285716 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0265371841799475,
            "unit": "iter/sec",
            "range": "stddev: 0.017939148166331607",
            "extra": "mean: 974.1488329999981 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5257.296163238501,
            "unit": "iter/sec",
            "range": "stddev: 0.00002681767206443595",
            "extra": "mean: 190.21184444438805 usec\nrounds: 2385"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4200.648912739057,
            "unit": "iter/sec",
            "range": "stddev: 0.00005098917591538406",
            "extra": "mean: 238.05845734152163 usec\nrounds: 2731"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 54.46924692942549,
            "unit": "iter/sec",
            "range": "stddev: 0.0008536866358139047",
            "extra": "mean: 18.358983396551753 msec\nrounds: 58"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.77994329780356,
            "unit": "iter/sec",
            "range": "stddev: 0.007287738820403848",
            "extra": "mean: 92.76486641666774 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1351.2541090618079,
            "unit": "iter/sec",
            "range": "stddev: 0.00011317777149040478",
            "extra": "mean: 740.0532537098534 usec\nrounds: 741"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 886.0194705967248,
            "unit": "iter/sec",
            "range": "stddev: 0.0001131716030280532",
            "extra": "mean: 1.12864336866831 msec\nrounds: 849"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.823328248561433,
            "unit": "iter/sec",
            "range": "stddev: 0.008147899548662551",
            "extra": "mean: 207.3257195999986 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9811042702362217,
            "unit": "iter/sec",
            "range": "stddev: 0.027616601205402384",
            "extra": "mean: 1.019259655000002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1322.304247806602,
            "unit": "iter/sec",
            "range": "stddev: 0.00007265058134304168",
            "extra": "mean: 756.2556058174733 usec\nrounds: 997"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 465.6985166336556,
            "unit": "iter/sec",
            "range": "stddev: 0.0012928180551547263",
            "extra": "mean: 2.147311971763603 msec\nrounds: 425"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.0370286003254252,
            "unit": "iter/sec",
            "range": "stddev: 0.022535606592481452",
            "extra": "mean: 964.2935592000015 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.19059947400618446,
            "unit": "iter/sec",
            "range": "stddev: 0.12331483434255644",
            "extra": "mean: 5.246604195599997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1340.2967979755804,
            "unit": "iter/sec",
            "range": "stddev: 0.0003514726056102291",
            "extra": "mean: 746.1034015081036 usec\nrounds: 1061"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 930.6634835892298,
            "unit": "iter/sec",
            "range": "stddev: 0.00011053074869302834",
            "extra": "mean: 1.0745022423608632 msec\nrounds: 949"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.894782540941612,
            "unit": "iter/sec",
            "range": "stddev: 0.010993996290898788",
            "extra": "mean: 169.64154199999777 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9827601276032105,
            "unit": "iter/sec",
            "range": "stddev: 0.022168485218363797",
            "extra": "mean: 1.0175422993999916 sec\nrounds: 5"
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
          "id": "e0b2788e219a04484c11eac4e998546b2a044fa0",
          "message": "query fixes (#81)\n\n* query fixes\r\n\r\n* add event to QueryByKey\r\n\r\n* use window tuples\r\n\r\n* add types to signuture\r\n\r\nCo-authored-by: Dina Nimrodi <dinan@iguazio.com>",
          "timestamp": "2020-12-01T14:47:29+02:00",
          "tree_id": "4e9a94f45b78918afcb03518270c135014acbc06",
          "url": "https://github.com/mlrun/storey/commit/e0b2788e219a04484c11eac4e998546b2a044fa0"
        },
        "date": 1606827056619,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1331.6265981650604,
            "unit": "iter/sec",
            "range": "stddev: 0.00005576244220422377",
            "extra": "mean: 750.9612690058674 usec\nrounds: 513"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 937.4267358642621,
            "unit": "iter/sec",
            "range": "stddev: 0.00006445284035329198",
            "extra": "mean: 1.0667500314870455 msec\nrounds: 921"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.461010463787175,
            "unit": "iter/sec",
            "range": "stddev: 0.00402457322670349",
            "extra": "mean: 154.77455200000432 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0374387297167182,
            "unit": "iter/sec",
            "range": "stddev: 0.02662149513676625",
            "extra": "mean: 963.91234620001 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4903.89706566727,
            "unit": "iter/sec",
            "range": "stddev: 0.000034712026850500985",
            "extra": "mean: 203.91945153194823 usec\nrounds: 2383"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3914.0585234756104,
            "unit": "iter/sec",
            "range": "stddev: 0.00004902392970484646",
            "extra": "mean: 255.48928152255084 usec\nrounds: 2522"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 53.84703102913766,
            "unit": "iter/sec",
            "range": "stddev: 0.0003932224938413081",
            "extra": "mean: 18.57112603773606 msec\nrounds: 53"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.80859532141887,
            "unit": "iter/sec",
            "range": "stddev: 0.007206271821916308",
            "extra": "mean: 92.51896016666923 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1310.1014881803974,
            "unit": "iter/sec",
            "range": "stddev: 0.000053112249519935864",
            "extra": "mean: 763.2996443572489 usec\nrounds: 762"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 834.4524142632231,
            "unit": "iter/sec",
            "range": "stddev: 0.00019759675403634237",
            "extra": "mean: 1.19839068460596 msec\nrounds: 799"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.591671345503309,
            "unit": "iter/sec",
            "range": "stddev: 0.0011640904530923195",
            "extra": "mean: 217.7856219999967 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9392374871504499,
            "unit": "iter/sec",
            "range": "stddev: 0.014847268983700972",
            "extra": "mean: 1.0646934493999993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1241.2686365237455,
            "unit": "iter/sec",
            "range": "stddev: 0.00005757353823662237",
            "extra": "mean: 805.62738038767 usec\nrounds: 928"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 441.7149508448553,
            "unit": "iter/sec",
            "range": "stddev: 0.0014231086724878705",
            "extra": "mean: 2.2639034474321713 msec\nrounds: 409"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9404183411681243,
            "unit": "iter/sec",
            "range": "stddev: 0.004368186896428311",
            "extra": "mean: 1.063356546999995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17870237909641018,
            "unit": "iter/sec",
            "range": "stddev: 0.039040849962595554",
            "extra": "mean: 5.595896400800006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1321.8902840721685,
            "unit": "iter/sec",
            "range": "stddev: 0.00005101650927515506",
            "extra": "mean: 756.4924351508473 usec\nrounds: 1064"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 891.002931178205,
            "unit": "iter/sec",
            "range": "stddev: 0.00006308802579569192",
            "extra": "mean: 1.1223307634663606 msec\nrounds: 854"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.025484943509591,
            "unit": "iter/sec",
            "range": "stddev: 0.0018189126715600491",
            "extra": "mean: 165.96174571428637 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0081238722675236,
            "unit": "iter/sec",
            "range": "stddev: 0.015642638966623625",
            "extra": "mean: 991.9415931999993 msec\nrounds: 5"
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
          "id": "fc9c2aaad90eda7b42cc8af231ae2b75b629a4f2",
          "message": "remove braces (#84)",
          "timestamp": "2020-12-02T11:12:23+02:00",
          "tree_id": "01eed3c224e8e48a72be9a20d75252984afa4ac1",
          "url": "https://github.com/mlrun/storey/commit/fc9c2aaad90eda7b42cc8af231ae2b75b629a4f2"
        },
        "date": 1606900572224,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1169.2233220397657,
            "unit": "iter/sec",
            "range": "stddev: 0.00015278402601219188",
            "extra": "mean: 855.2686053639885 usec\nrounds: 522"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 743.0455044457257,
            "unit": "iter/sec",
            "range": "stddev: 0.0009104276954533157",
            "extra": "mean: 1.3458125969632901 msec\nrounds: 856"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.356621528735493,
            "unit": "iter/sec",
            "range": "stddev: 0.014977916448519165",
            "extra": "mean: 186.68483383332557 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9442724765312068,
            "unit": "iter/sec",
            "range": "stddev: 0.08964490524669311",
            "extra": "mean: 1.0590163590000088 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4298.185975100798,
            "unit": "iter/sec",
            "range": "stddev: 0.00012061722005392445",
            "extra": "mean: 232.6562893725297 usec\nrounds: 2343"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3301.1082346240264,
            "unit": "iter/sec",
            "range": "stddev: 0.00013199579331530943",
            "extra": "mean: 302.9285709300268 usec\nrounds: 2291"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 50.075740086526004,
            "unit": "iter/sec",
            "range": "stddev: 0.0018160640463995526",
            "extra": "mean: 19.969749788462384 msec\nrounds: 52"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.420795877428805,
            "unit": "iter/sec",
            "range": "stddev: 0.009064373833261856",
            "extra": "mean: 95.96196027272507 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1142.1379059853143,
            "unit": "iter/sec",
            "range": "stddev: 0.00023566964297159565",
            "extra": "mean: 875.5510124999372 usec\nrounds: 720"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 715.1911022211154,
            "unit": "iter/sec",
            "range": "stddev: 0.00025749139981693053",
            "extra": "mean: 1.3982276861308465 msec\nrounds: 685"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.6193059972354225,
            "unit": "iter/sec",
            "range": "stddev: 0.01458394407754345",
            "extra": "mean: 276.29606359999457 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.771718462582961,
            "unit": "iter/sec",
            "range": "stddev: 0.09665330726441389",
            "extra": "mean: 1.295809350799999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1122.518676990508,
            "unit": "iter/sec",
            "range": "stddev: 0.00021005681741997125",
            "extra": "mean: 890.8537741937776 usec\nrounds: 310"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 401.5308503191571,
            "unit": "iter/sec",
            "range": "stddev: 0.000522739864841494",
            "extra": "mean: 2.49046866313049 msec\nrounds: 377"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8926053347479757,
            "unit": "iter/sec",
            "range": "stddev: 0.046676553195532045",
            "extra": "mean: 1.1203159571999948 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.161683931132126,
            "unit": "iter/sec",
            "range": "stddev: 0.12279235285710595",
            "extra": "mean: 6.184906520999993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1071.227384396037,
            "unit": "iter/sec",
            "range": "stddev: 0.00039273633651455735",
            "extra": "mean: 933.5086225076337 usec\nrounds: 702"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 727.4370653626369,
            "unit": "iter/sec",
            "range": "stddev: 0.0005915321973735943",
            "extra": "mean: 1.3746893684905743 msec\nrounds: 768"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.586115138061112,
            "unit": "iter/sec",
            "range": "stddev: 0.010149346230496837",
            "extra": "mean: 179.01528616667406 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9005963826517024,
            "unit": "iter/sec",
            "range": "stddev: 0.07109009718931335",
            "extra": "mean: 1.110375323800008 sec\nrounds: 5"
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
          "id": "9aa235fd92f05264b3cc9e2b15c97ef914fd3937",
          "message": "Writer inference fixes. (#82)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-02T11:59:17+02:00",
          "tree_id": "bc7fdfc74b00014ae9cd74e5bc7514bfe7c99511",
          "url": "https://github.com/mlrun/storey/commit/9aa235fd92f05264b3cc9e2b15c97ef914fd3937"
        },
        "date": 1606903381169,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1254.7405238554325,
            "unit": "iter/sec",
            "range": "stddev: 0.00006996087441446218",
            "extra": "mean: 796.9775272159911 usec\nrounds: 643"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 881.9650085288442,
            "unit": "iter/sec",
            "range": "stddev: 0.00013139327356828765",
            "extra": "mean: 1.1338318304351362 msec\nrounds: 690"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.175590878947332,
            "unit": "iter/sec",
            "range": "stddev: 0.0022025938454720323",
            "extra": "mean: 161.927825142856 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9914557470017145,
            "unit": "iter/sec",
            "range": "stddev: 0.024551188077803435",
            "extra": "mean: 1.0086178863999975 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4552.826519252984,
            "unit": "iter/sec",
            "range": "stddev: 0.000013705211195502635",
            "extra": "mean: 219.64377420734172 usec\nrounds: 2334"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3654.1599181740253,
            "unit": "iter/sec",
            "range": "stddev: 0.000035855851145386744",
            "extra": "mean: 273.66071064007986 usec\nrounds: 2312"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 49.72365684064655,
            "unit": "iter/sec",
            "range": "stddev: 0.00006962646056515824",
            "extra": "mean: 20.111151583335502 msec\nrounds: 48"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.792279072461895,
            "unit": "iter/sec",
            "range": "stddev: 0.007912457840021704",
            "extra": "mean: 102.12127254545128 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1262.2288354236853,
            "unit": "iter/sec",
            "range": "stddev: 0.00007181108703464058",
            "extra": "mean: 792.249370269168 usec\nrounds: 740"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 820.7676877797867,
            "unit": "iter/sec",
            "range": "stddev: 0.00011917066151774499",
            "extra": "mean: 1.2183715500607055 msec\nrounds: 829"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.414697627130864,
            "unit": "iter/sec",
            "range": "stddev: 0.007138693874771498",
            "extra": "mean: 226.5160797999897 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9098826567193111,
            "unit": "iter/sec",
            "range": "stddev: 0.019866699095854562",
            "extra": "mean: 1.0990428189999988 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1203.8433079124459,
            "unit": "iter/sec",
            "range": "stddev: 0.00010802539315344075",
            "extra": "mean: 830.6728902568512 usec\nrounds: 975"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 438.7689437180004,
            "unit": "iter/sec",
            "range": "stddev: 0.000058408462022231583",
            "extra": "mean: 2.2791038753251103 msec\nrounds: 385"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.909526070020061,
            "unit": "iter/sec",
            "range": "stddev: 0.01008387293774405",
            "extra": "mean: 1.0994737071999965 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16834680067161498,
            "unit": "iter/sec",
            "range": "stddev: 0.05996777596096727",
            "extra": "mean: 5.940118826200006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1217.0015056618195,
            "unit": "iter/sec",
            "range": "stddev: 0.00017078160235120508",
            "extra": "mean: 821.6916703452955 usec\nrounds: 725"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 823.8836296279801,
            "unit": "iter/sec",
            "range": "stddev: 0.0006717506382744326",
            "extra": "mean: 1.2137636482127265 msec\nrounds: 867"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.704522874315006,
            "unit": "iter/sec",
            "range": "stddev: 0.003154214585725054",
            "extra": "mean: 175.29949866667494 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9527354289393807,
            "unit": "iter/sec",
            "range": "stddev: 0.03256342980964113",
            "extra": "mean: 1.0496093349999966 sec\nrounds: 5"
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
          "id": "0c56a76f3b40df0f05fb63e8cd00a7c16a13268f",
          "message": "validate range when aggregating and getting features (#86)\n\n* validate range when aggregating and getting features\r\n\r\n* add tests",
          "timestamp": "2020-12-03T13:40:15+02:00",
          "tree_id": "f14540f71d228987a4d8f0e9289da8f0b99941f1",
          "url": "https://github.com/mlrun/storey/commit/0c56a76f3b40df0f05fb63e8cd00a7c16a13268f"
        },
        "date": 1606995866372,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 984.67225110627,
            "unit": "iter/sec",
            "range": "stddev: 0.0006477991141902461",
            "extra": "mean: 1.0155663459354212 msec\nrounds: 529"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 690.1055431804201,
            "unit": "iter/sec",
            "range": "stddev: 0.0006476676173034517",
            "extra": "mean: 1.449053713423892 msec\nrounds: 663"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.326673765244137,
            "unit": "iter/sec",
            "range": "stddev: 0.006938083929248112",
            "extra": "mean: 231.12442820000183 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6805849474464465,
            "unit": "iter/sec",
            "range": "stddev: 0.013344155871232947",
            "extra": "mean: 1.4693242978 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3315.710593852401,
            "unit": "iter/sec",
            "range": "stddev: 0.00012033305410827095",
            "extra": "mean: 301.59447626523314 usec\nrounds: 1917"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2689.4892767038837,
            "unit": "iter/sec",
            "range": "stddev: 0.00018279027872699063",
            "extra": "mean: 371.81780521004896 usec\nrounds: 2572"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 47.10252943751305,
            "unit": "iter/sec",
            "range": "stddev: 0.0009998290112494574",
            "extra": "mean: 21.230282363638572 msec\nrounds: 44"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 8.90386613267608,
            "unit": "iter/sec",
            "range": "stddev: 0.01157987652350575",
            "extra": "mean: 112.31076311110793 msec\nrounds: 9"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1057.1328205637515,
            "unit": "iter/sec",
            "range": "stddev: 0.0002689646738019547",
            "extra": "mean: 945.9549269000242 usec\nrounds: 684"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 644.045476235228,
            "unit": "iter/sec",
            "range": "stddev: 0.001039926694394548",
            "extra": "mean: 1.5526853877547695 msec\nrounds: 637"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.891519044812384,
            "unit": "iter/sec",
            "range": "stddev: 0.012556210132018217",
            "extra": "mean: 345.83898099999715 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5978487090967901,
            "unit": "iter/sec",
            "range": "stddev: 0.03364256504197357",
            "extra": "mean: 1.672663977999997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 976.6097368629323,
            "unit": "iter/sec",
            "range": "stddev: 0.0005902014856029686",
            "extra": "mean: 1.0239504709549607 msec\nrounds: 964"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 375.5021133796409,
            "unit": "iter/sec",
            "range": "stddev: 0.0008894228928071075",
            "extra": "mean: 2.663100857142122 msec\nrounds: 406"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8138768511158121,
            "unit": "iter/sec",
            "range": "stddev: 0.04987224204672617",
            "extra": "mean: 1.2286871148000045 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15057152209041155,
            "unit": "iter/sec",
            "range": "stddev: 0.021193397911007855",
            "extra": "mean: 6.641362098999997 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1057.0500493297097,
            "unit": "iter/sec",
            "range": "stddev: 0.0005456599551198939",
            "extra": "mean: 946.0289989429679 usec\nrounds: 947"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 650.5423162837385,
            "unit": "iter/sec",
            "range": "stddev: 0.000841717413911219",
            "extra": "mean: 1.537179019671709 msec\nrounds: 610"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.199403311150835,
            "unit": "iter/sec",
            "range": "stddev: 0.013571884910580439",
            "extra": "mean: 238.12906880000355 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6403231494762299,
            "unit": "iter/sec",
            "range": "stddev: 0.05550739078864653",
            "extra": "mean: 1.5617114590000027 sec\nrounds: 5"
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
          "id": "2675bafa720ccbbad27c1b4d23f39245c97789a0",
          "message": "Fixes in CSV reader and writer. (#85)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-06T10:49:56+02:00",
          "tree_id": "99000cf7988d0e6c76623c40e80d93587ecb7ada",
          "url": "https://github.com/mlrun/storey/commit/2675bafa720ccbbad27c1b4d23f39245c97789a0"
        },
        "date": 1607244840071,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 965.6987906168948,
            "unit": "iter/sec",
            "range": "stddev: 0.0010339453953447718",
            "extra": "mean: 1.0355195737184193 msec\nrounds: 624"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 631.9025520120633,
            "unit": "iter/sec",
            "range": "stddev: 0.0012111627100367859",
            "extra": "mean: 1.5825224899248542 msec\nrounds: 794"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.324550154671026,
            "unit": "iter/sec",
            "range": "stddev: 0.03194725024359458",
            "extra": "mean: 231.23792399999843 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.708600856410292,
            "unit": "iter/sec",
            "range": "stddev: 0.12111471802103188",
            "extra": "mean: 1.4112317123999958 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3932.9585416657224,
            "unit": "iter/sec",
            "range": "stddev: 0.00014962010230241114",
            "extra": "mean: 254.26151570274905 usec\nrounds: 1815"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2924.2177586897637,
            "unit": "iter/sec",
            "range": "stddev: 0.00037619799810178855",
            "extra": "mean: 341.9717963986594 usec\nrounds: 722"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 42.90662030561823,
            "unit": "iter/sec",
            "range": "stddev: 0.002908877578444619",
            "extra": "mean: 23.306426674418333 msec\nrounds: 43"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.250823981067457,
            "unit": "iter/sec",
            "range": "stddev: 0.009923772910387686",
            "extra": "mean: 108.09847880000518 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 990.4569421560748,
            "unit": "iter/sec",
            "range": "stddev: 0.0007965109144383063",
            "extra": "mean: 1.0096350052564136 msec\nrounds: 761"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 692.0299670313794,
            "unit": "iter/sec",
            "range": "stddev: 0.0009247872546552453",
            "extra": "mean: 1.4450241284921927 msec\nrounds: 716"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.3457299004942884,
            "unit": "iter/sec",
            "range": "stddev: 0.05101962863050927",
            "extra": "mean: 426.3065410000024 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.5769543101893239,
            "unit": "iter/sec",
            "range": "stddev: 0.12454497574976542",
            "extra": "mean: 1.733239499800004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1044.3249912978781,
            "unit": "iter/sec",
            "range": "stddev: 0.0005503407367709927",
            "extra": "mean: 957.5563242599495 usec\nrounds: 845"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 352.39390720114886,
            "unit": "iter/sec",
            "range": "stddev: 0.0014700611025193328",
            "extra": "mean: 2.837733512314085 msec\nrounds: 406"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7760743480467103,
            "unit": "iter/sec",
            "range": "stddev: 0.03838251321966373",
            "extra": "mean: 1.2885363400000074 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15207398464830096,
            "unit": "iter/sec",
            "range": "stddev: 0.4625747969375472",
            "extra": "mean: 6.575746682200008 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1240.2292686278151,
            "unit": "iter/sec",
            "range": "stddev: 0.00014060188017883419",
            "extra": "mean: 806.3025323587116 usec\nrounds: 479"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 797.4348617409563,
            "unit": "iter/sec",
            "range": "stddev: 0.00036473104138983527",
            "extra": "mean: 1.2540209213035964 msec\nrounds: 737"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.031524939844831,
            "unit": "iter/sec",
            "range": "stddev: 0.010067645402183937",
            "extra": "mean: 198.74690316666488 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9138878514236021,
            "unit": "iter/sec",
            "range": "stddev: 0.04928535099943016",
            "extra": "mean: 1.0942261661999964 sec\nrounds: 5"
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
          "id": "176a6cc0c6d9ac88a9e83f5f793c25c18a04dbe0",
          "message": "fix loading aggregations from storage (#87)\n\n* fix loading aggregations from storage\r\n\r\n* add unit tests + fixes\r\n\r\n* review comments",
          "timestamp": "2020-12-06T12:30:02+02:00",
          "tree_id": "78e86d18e7fb5f1a2fefddce637102332c59b0c7",
          "url": "https://github.com/mlrun/storey/commit/176a6cc0c6d9ac88a9e83f5f793c25c18a04dbe0"
        },
        "date": 1607250828356,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1204.4510851200944,
            "unit": "iter/sec",
            "range": "stddev: 0.00011806627761426554",
            "extra": "mean: 830.2537249989618 usec\nrounds: 440"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 857.9362526973607,
            "unit": "iter/sec",
            "range": "stddev: 0.000317864402050211",
            "extra": "mean: 1.1655877658229146 msec\nrounds: 790"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.648462917112507,
            "unit": "iter/sec",
            "range": "stddev: 0.009225348260308294",
            "extra": "mean: 177.03931399999342 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9013563349817088,
            "unit": "iter/sec",
            "range": "stddev: 0.031863561437709964",
            "extra": "mean: 1.1094391432000008 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4533.969877341636,
            "unit": "iter/sec",
            "range": "stddev: 0.00004567254321319892",
            "extra": "mean: 220.55726593982615 usec\nrounds: 2384"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3645.8377043570713,
            "unit": "iter/sec",
            "range": "stddev: 0.00007281140673194981",
            "extra": "mean: 274.28538544239615 usec\nrounds: 2514"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 50.77697568856718,
            "unit": "iter/sec",
            "range": "stddev: 0.0006863872077057729",
            "extra": "mean: 19.693965354166565 msec\nrounds: 48"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.874308703687502,
            "unit": "iter/sec",
            "range": "stddev: 0.009702459356299414",
            "extra": "mean: 101.27291236363268 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1223.2638225527792,
            "unit": "iter/sec",
            "range": "stddev: 0.00014737978114287762",
            "extra": "mean: 817.4851422591252 usec\nrounds: 478"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 754.1221153556836,
            "unit": "iter/sec",
            "range": "stddev: 0.0005341326913119839",
            "extra": "mean: 1.3260451850405521 msec\nrounds: 762"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.047956391968171,
            "unit": "iter/sec",
            "range": "stddev: 0.010064851344950772",
            "extra": "mean: 247.03823439999724 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8405831993741206,
            "unit": "iter/sec",
            "range": "stddev: 0.04860413884499681",
            "extra": "mean: 1.1896502341999906 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1196.9691435967136,
            "unit": "iter/sec",
            "range": "stddev: 0.000228080635410285",
            "extra": "mean: 835.4434242098749 usec\nrounds: 917"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 407.34699466069816,
            "unit": "iter/sec",
            "range": "stddev: 0.0016526441455282093",
            "extra": "mean: 2.4549094828426448 msec\nrounds: 408"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9241488709915533,
            "unit": "iter/sec",
            "range": "stddev: 0.018294621894394743",
            "extra": "mean: 1.0820767425999918 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16633361915090022,
            "unit": "iter/sec",
            "range": "stddev: 0.051828167265168895",
            "extra": "mean: 6.012013717400004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1245.402520732652,
            "unit": "iter/sec",
            "range": "stddev: 0.00008422198129144523",
            "extra": "mean: 802.9532487309522 usec\nrounds: 985"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 828.4986994662286,
            "unit": "iter/sec",
            "range": "stddev: 0.0004685541512270646",
            "extra": "mean: 1.2070024981864949 msec\nrounds: 827"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.358218859251922,
            "unit": "iter/sec",
            "range": "stddev: 0.006324714010720513",
            "extra": "mean: 186.62918149999066 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9169887746848592,
            "unit": "iter/sec",
            "range": "stddev: 0.032134903692055426",
            "extra": "mean: 1.090525890399988 sec\nrounds: 5"
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
          "id": "aad9b46c7d5c66f7bab30e0b040216aa265600ae",
          "message": "Make DataframeSource fast. (#88)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-07T14:10:11+02:00",
          "tree_id": "ecee4f8aeef0ab2bbf7455cd55cc47c040543612",
          "url": "https://github.com/mlrun/storey/commit/aad9b46c7d5c66f7bab30e0b040216aa265600ae"
        },
        "date": 1607343233524,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1236.2443544355365,
            "unit": "iter/sec",
            "range": "stddev: 0.00009926317942589377",
            "extra": "mean: 808.9015706418295 usec\nrounds: 545"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 860.4875794506424,
            "unit": "iter/sec",
            "range": "stddev: 0.00019745101124743722",
            "extra": "mean: 1.162131823725365 msec\nrounds: 902"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.932420787077322,
            "unit": "iter/sec",
            "range": "stddev: 0.0010690768544483445",
            "extra": "mean: 168.5652511666594 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9390606609488916,
            "unit": "iter/sec",
            "range": "stddev: 0.01774919561742131",
            "extra": "mean: 1.064893932399991 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4881.874675033862,
            "unit": "iter/sec",
            "range": "stddev: 0.000019462393136979416",
            "extra": "mean: 204.83934278650108 usec\nrounds: 2433"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3856.134410003553,
            "unit": "iter/sec",
            "range": "stddev: 0.00004937366006009192",
            "extra": "mean: 259.3270601267964 usec\nrounds: 2528"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 54.054396297497796,
            "unit": "iter/sec",
            "range": "stddev: 0.0003856666435231375",
            "extra": "mean: 18.499882867923 msec\nrounds: 53"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.50414560907754,
            "unit": "iter/sec",
            "range": "stddev: 0.007885441068624632",
            "extra": "mean: 95.20050818181858 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1253.766404369548,
            "unit": "iter/sec",
            "range": "stddev: 0.00023433032523350233",
            "extra": "mean: 797.5967425150832 usec\nrounds: 835"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 817.0400060811659,
            "unit": "iter/sec",
            "range": "stddev: 0.00015801910872257512",
            "extra": "mean: 1.2239302758213515 msec\nrounds: 852"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.236349155983023,
            "unit": "iter/sec",
            "range": "stddev: 0.0032796726613989243",
            "extra": "mean: 236.05230900000151 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8438337494712111,
            "unit": "iter/sec",
            "range": "stddev: 0.04314848925358135",
            "extra": "mean: 1.1850675570000022 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1207.9485251199146,
            "unit": "iter/sec",
            "range": "stddev: 0.00010141389946210007",
            "extra": "mean: 827.8498455890152 usec\nrounds: 816"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 447.11973232596625,
            "unit": "iter/sec",
            "range": "stddev: 0.00018977388563816303",
            "extra": "mean: 2.236537391892524 msec\nrounds: 370"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9100916274771768,
            "unit": "iter/sec",
            "range": "stddev: 0.03831624511783622",
            "extra": "mean: 1.098790462200003 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16872143844958182,
            "unit": "iter/sec",
            "range": "stddev: 0.06652596534297903",
            "extra": "mean: 5.9269290801999945 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1260.5338489327282,
            "unit": "iter/sec",
            "range": "stddev: 0.00015577076962261765",
            "extra": "mean: 793.3146744505771 usec\nrounds: 728"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 840.2475852482345,
            "unit": "iter/sec",
            "range": "stddev: 0.0002803080963167804",
            "extra": "mean: 1.1901254077446348 msec\nrounds: 878"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.279638318165834,
            "unit": "iter/sec",
            "range": "stddev: 0.019088793747249252",
            "extra": "mean: 189.4069138333331 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9047531453124764,
            "unit": "iter/sec",
            "range": "stddev: 0.03736123076833809",
            "extra": "mean: 1.105273858599992 sec\nrounds: 5"
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
          "id": "9abea3c7210169e5059f5fdb1e22f9fc18f6f366",
          "message": "Remove unnecessary async iteration. (#89)\n\nIteration can be suspended on await self._do_downstream(event) anyway.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-07T15:19:09+02:00",
          "tree_id": "18a24b32d248c1aa5ce330bda756c83c1527a625",
          "url": "https://github.com/mlrun/storey/commit/9abea3c7210169e5059f5fdb1e22f9fc18f6f366"
        },
        "date": 1607347379367,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1260.3310797937809,
            "unit": "iter/sec",
            "range": "stddev: 0.00012783220455147917",
            "extra": "mean: 793.4423073686503 usec\nrounds: 475"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 874.4526013804114,
            "unit": "iter/sec",
            "range": "stddev: 0.0003208715937231825",
            "extra": "mean: 1.1435725600465931 msec\nrounds: 866"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.445529224481949,
            "unit": "iter/sec",
            "range": "stddev: 0.01306414034262623",
            "extra": "mean: 183.63688059999959 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9059563626694266,
            "unit": "iter/sec",
            "range": "stddev: 0.045542597666554066",
            "extra": "mean: 1.1038059239999938 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4826.173554339403,
            "unit": "iter/sec",
            "range": "stddev: 0.000011872395803227386",
            "extra": "mean: 207.20348921162613 usec\nrounds: 2410"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3892.3422704916884,
            "unit": "iter/sec",
            "range": "stddev: 0.000050907833624075286",
            "extra": "mean: 256.9147136882384 usec\nrounds: 2389"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 52.526777446568026,
            "unit": "iter/sec",
            "range": "stddev: 0.0001007424787861225",
            "extra": "mean: 19.037908826926095 msec\nrounds: 52"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.349813983827465,
            "unit": "iter/sec",
            "range": "stddev: 0.009354704379435787",
            "extra": "mean: 96.62009400000731 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1212.9718458682758,
            "unit": "iter/sec",
            "range": "stddev: 0.0002931619788331717",
            "extra": "mean: 824.4214434211989 usec\nrounds: 760"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 794.5671982650217,
            "unit": "iter/sec",
            "range": "stddev: 0.0003988102875601674",
            "extra": "mean: 1.2585467940075443 msec\nrounds: 801"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.9078667241532004,
            "unit": "iter/sec",
            "range": "stddev: 0.026897801807360726",
            "extra": "mean: 255.8940901999904 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9109126371459808,
            "unit": "iter/sec",
            "range": "stddev: 0.013093528112577258",
            "extra": "mean: 1.0978001173999985 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1115.6550752456437,
            "unit": "iter/sec",
            "range": "stddev: 0.0004079291845264405",
            "extra": "mean: 896.33437985286 usec\nrounds: 953"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 416.51034060990617,
            "unit": "iter/sec",
            "range": "stddev: 0.0006984253565102905",
            "extra": "mean: 2.4009007760423806 msec\nrounds: 384"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8733779184679086,
            "unit": "iter/sec",
            "range": "stddev: 0.03818003400657884",
            "extra": "mean: 1.1449797147999958 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16002876595674234,
            "unit": "iter/sec",
            "range": "stddev: 0.06526818993921536",
            "extra": "mean: 6.248876531799988 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1228.4958229991687,
            "unit": "iter/sec",
            "range": "stddev: 0.00027049181627643407",
            "extra": "mean: 814.0035816798026 usec\nrounds: 655"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 809.1323332387545,
            "unit": "iter/sec",
            "range": "stddev: 0.000501298210355312",
            "extra": "mean: 1.2358917805166059 msec\nrounds: 852"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.102287405190896,
            "unit": "iter/sec",
            "range": "stddev: 0.012001721058263063",
            "extra": "mean: 195.99052750000587 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.8366960430385147,
            "unit": "iter/sec",
            "range": "stddev: 0.03055227344292647",
            "extra": "mean: 1.1951771594000093 sec\nrounds: 5"
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
          "id": "7f2551f6ea5f5fbf0dfebc85190163ae3c6f5841",
          "message": "Better error handling in ReadCSV. (#90)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-07T18:03:31+02:00",
          "tree_id": "b38cae58ccb6358b1d1b21c42d7cf0f5b414e5a8",
          "url": "https://github.com/mlrun/storey/commit/7f2551f6ea5f5fbf0dfebc85190163ae3c6f5841"
        },
        "date": 1607357212897,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1388.4917034479522,
            "unit": "iter/sec",
            "range": "stddev: 0.000054613835025637564",
            "extra": "mean: 720.2059598316391 usec\nrounds: 473"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 987.0524622819535,
            "unit": "iter/sec",
            "range": "stddev: 0.00008624793383872239",
            "extra": "mean: 1.0131173754312037 msec\nrounds: 871"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.899691688854041,
            "unit": "iter/sec",
            "range": "stddev: 0.0025278865282828116",
            "extra": "mean: 144.9340122857125 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0681493714744672,
            "unit": "iter/sec",
            "range": "stddev: 0.011469429191772074",
            "extra": "mean: 936.1986503999958 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5144.245884395062,
            "unit": "iter/sec",
            "range": "stddev: 0.000025321819628490465",
            "extra": "mean: 194.39195218748668 usec\nrounds: 2217"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4280.172342593306,
            "unit": "iter/sec",
            "range": "stddev: 0.00005820183946614666",
            "extra": "mean: 233.6354520234369 usec\nrounds: 2199"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 57.77755022206818,
            "unit": "iter/sec",
            "range": "stddev: 0.0011603564589465947",
            "extra": "mean: 17.30776047368739 msec\nrounds: 57"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.433399770883899,
            "unit": "iter/sec",
            "range": "stddev: 0.006630518323580901",
            "extra": "mean: 87.46304861538935 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1396.3787458445277,
            "unit": "iter/sec",
            "range": "stddev: 0.00006141253877037967",
            "extra": "mean: 716.138084295462 usec\nrounds: 866"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 875.1640711917015,
            "unit": "iter/sec",
            "range": "stddev: 0.00013255323622917697",
            "extra": "mean: 1.142642885965726 msec\nrounds: 798"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.609230834134436,
            "unit": "iter/sec",
            "range": "stddev: 0.0038478566229931148",
            "extra": "mean: 216.95593819999885 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9950674470825711,
            "unit": "iter/sec",
            "range": "stddev: 0.024932140035771006",
            "extra": "mean: 1.0049570035999977 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1337.6258332409077,
            "unit": "iter/sec",
            "range": "stddev: 0.0001086096512055886",
            "extra": "mean: 747.5932171383977 usec\nrounds: 1202"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 466.7758450747712,
            "unit": "iter/sec",
            "range": "stddev: 0.0011964554118027411",
            "extra": "mean: 2.142355930692629 msec\nrounds: 404"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.030818359591282,
            "unit": "iter/sec",
            "range": "stddev: 0.02987087555504195",
            "extra": "mean: 970.1030163999974 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.1948688139153811,
            "unit": "iter/sec",
            "range": "stddev: 0.09439080332895874",
            "extra": "mean: 5.131657446399993 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1394.7281166026453,
            "unit": "iter/sec",
            "range": "stddev: 0.00006947401660146836",
            "extra": "mean: 716.9856175523689 usec\nrounds: 1242"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 932.7539676047279,
            "unit": "iter/sec",
            "range": "stddev: 0.0004334392213862734",
            "extra": "mean: 1.072094072746704 msec\nrounds: 921"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.265782207468646,
            "unit": "iter/sec",
            "range": "stddev: 0.010105674246674386",
            "extra": "mean: 159.59699314285558 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0523116693321657,
            "unit": "iter/sec",
            "range": "stddev: 0.012926468125699803",
            "extra": "mean: 950.2888062000068 msec\nrounds: 5"
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
          "id": "cda1113999838435cc8c0b32744e8b1ade5d358e",
          "message": "Reify metadata in WriteToV3IOStream. (#80)\n\n* Reify metadata in WriteToV3IOStream.\r\n\r\n* Make WriteToV3IOStream a Writer.\r\n\r\n* Update docstring. Test inference.\r\n\r\n* Setup and teardown in tests.\r\n\r\n* Add test.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-08T11:38:41+02:00",
          "tree_id": "762b5cbd3b5f3048f09887880b284d4e03a7ce37",
          "url": "https://github.com/mlrun/storey/commit/cda1113999838435cc8c0b32744e8b1ade5d358e"
        },
        "date": 1607420551765,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1179.4562021338425,
            "unit": "iter/sec",
            "range": "stddev: 0.00011528200564142525",
            "extra": "mean: 847.8483543439978 usec\nrounds: 587"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 810.6567958409107,
            "unit": "iter/sec",
            "range": "stddev: 0.00014745445556673943",
            "extra": "mean: 1.233567651724525 msec\nrounds: 580"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.076685775447414,
            "unit": "iter/sec",
            "range": "stddev: 0.005291953021251602",
            "extra": "mean: 196.9789039999957 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7364892057927399,
            "unit": "iter/sec",
            "range": "stddev: 0.04334335591955018",
            "extra": "mean: 1.3577931517999957 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3387.90208437281,
            "unit": "iter/sec",
            "range": "stddev: 0.0002130822602876547",
            "extra": "mean: 295.1679166327283 usec\nrounds: 2459"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2866.1333090769804,
            "unit": "iter/sec",
            "range": "stddev: 0.00016373572963785543",
            "extra": "mean: 348.90212427768876 usec\nrounds: 2422"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 48.609796524745725,
            "unit": "iter/sec",
            "range": "stddev: 0.0005169580450621341",
            "extra": "mean: 20.571984897960462 msec\nrounds: 49"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.52184296397955,
            "unit": "iter/sec",
            "range": "stddev: 0.008276345666812485",
            "extra": "mean: 105.02168579999989 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1187.7726494231556,
            "unit": "iter/sec",
            "range": "stddev: 0.00031043977105409213",
            "extra": "mean: 841.9119605807157 usec\nrounds: 964"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 806.7779124977977,
            "unit": "iter/sec",
            "range": "stddev: 0.00011813896030862366",
            "extra": "mean: 1.2394984846622579 msec\nrounds: 652"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.648405108536063,
            "unit": "iter/sec",
            "range": "stddev: 0.01424291171653466",
            "extra": "mean: 274.092369199991 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.7304017346257088,
            "unit": "iter/sec",
            "range": "stddev: 0.022506778182346095",
            "extra": "mean: 1.3691095633999908 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1137.700100304368,
            "unit": "iter/sec",
            "range": "stddev: 0.00043560007543079547",
            "extra": "mean: 878.9662580960227 usec\nrounds: 1019"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 439.04674654856933,
            "unit": "iter/sec",
            "range": "stddev: 0.00014078531632882846",
            "extra": "mean: 2.2776617931033356 msec\nrounds: 406"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8826635275225488,
            "unit": "iter/sec",
            "range": "stddev: 0.017496552142423776",
            "extra": "mean: 1.1329345428000068 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.16545327629922366,
            "unit": "iter/sec",
            "range": "stddev: 0.043989437984887865",
            "extra": "mean: 6.044002405800001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1172.425055099122,
            "unit": "iter/sec",
            "range": "stddev: 0.0004212876971558483",
            "extra": "mean: 852.9329833500153 usec\nrounds: 1021"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 793.5435874679763,
            "unit": "iter/sec",
            "range": "stddev: 0.00016115738384270978",
            "extra": "mean: 1.2601702235295997 msec\nrounds: 765"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.377558119109885,
            "unit": "iter/sec",
            "range": "stddev: 0.026456974922970757",
            "extra": "mean: 228.4378580000066 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7478896509408007,
            "unit": "iter/sec",
            "range": "stddev: 0.013234699235490569",
            "extra": "mean: 1.3370956513999885 sec\nrounds: 5"
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
          "id": "dd7e73f0acea174f4aabd8547336a82287235599",
          "message": "Infer types in ReadCSV. (#91)\n\n* Infer types in ReadCSV.\r\n\r\n* Add type annotation and docstring line.\r\n\r\n* Add boolean inference.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-09T10:05:09+02:00",
          "tree_id": "35a6d66be88aa59cc08354d9776a8dcd3144a97b",
          "url": "https://github.com/mlrun/storey/commit/dd7e73f0acea174f4aabd8547336a82287235599"
        },
        "date": 1607501352884,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1325.6480579259523,
            "unit": "iter/sec",
            "range": "stddev: 0.00016108758503659247",
            "extra": "mean: 754.3480292684574 usec\nrounds: 615"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 936.4713214508928,
            "unit": "iter/sec",
            "range": "stddev: 0.00012233886974892316",
            "extra": "mean: 1.067838359909069 msec\nrounds: 878"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.478988102900727,
            "unit": "iter/sec",
            "range": "stddev: 0.006041860362263889",
            "extra": "mean: 154.3450897142853 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0874577106672445,
            "unit": "iter/sec",
            "range": "stddev: 0.025040279506683024",
            "extra": "mean: 919.5759892000012 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5199.251967925555,
            "unit": "iter/sec",
            "range": "stddev: 0.000024737236020214932",
            "extra": "mean: 192.33536019586083 usec\nrounds: 2457"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4113.044068459154,
            "unit": "iter/sec",
            "range": "stddev: 0.00003939863609619256",
            "extra": "mean: 243.12892917158175 usec\nrounds: 2499"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 51.941089271125456,
            "unit": "iter/sec",
            "range": "stddev: 0.0036794684880481253",
            "extra": "mean: 19.252580452830614 msec\nrounds: 53"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.963198539631923,
            "unit": "iter/sec",
            "range": "stddev: 0.0011819648334770755",
            "extra": "mean: 91.21425616666556 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1316.1125273581938,
            "unit": "iter/sec",
            "range": "stddev: 0.00013180823665329697",
            "extra": "mean: 759.813449999811 usec\nrounds: 800"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 781.7780641702134,
            "unit": "iter/sec",
            "range": "stddev: 0.001165061419538196",
            "extra": "mean: 1.2791354040630565 msec\nrounds: 886"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.4898615733383,
            "unit": "iter/sec",
            "range": "stddev: 0.0075318515074059705",
            "extra": "mean: 222.72401579999723 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9189614335817962,
            "unit": "iter/sec",
            "range": "stddev: 0.028068026098947377",
            "extra": "mean: 1.088184948199995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1264.0516209807943,
            "unit": "iter/sec",
            "range": "stddev: 0.00023840372264356347",
            "extra": "mean: 791.1069321869046 usec\nrounds: 1047"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 457.08685747295283,
            "unit": "iter/sec",
            "range": "stddev: 0.0012732276704925576",
            "extra": "mean: 2.187768000000247 msec\nrounds: 433"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.953008362259551,
            "unit": "iter/sec",
            "range": "stddev: 0.022778749907422582",
            "extra": "mean: 1.0493087360000004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17850473444039047,
            "unit": "iter/sec",
            "range": "stddev: 0.1482387276763168",
            "extra": "mean: 5.602092309399995 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1270.130714826867,
            "unit": "iter/sec",
            "range": "stddev: 0.00020636064387245944",
            "extra": "mean: 787.3205397889391 usec\nrounds: 1043"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 883.7065619203772,
            "unit": "iter/sec",
            "range": "stddev: 0.00013593385751766547",
            "extra": "mean: 1.1315973458733928 msec\nrounds: 824"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.019761949717367,
            "unit": "iter/sec",
            "range": "stddev: 0.003576907514683579",
            "extra": "mean: 166.11952571429356 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0556700881592895,
            "unit": "iter/sec",
            "range": "stddev: 0.09085450009437733",
            "extra": "mean: 947.2656384000061 msec\nrounds: 5"
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
          "id": "fd2b7638b48ee2923ae772c93aba5a2cef7605dc",
          "message": "Add release to PyPi support. (#93)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-10T13:21:13+02:00",
          "tree_id": "929c363f5e7e7bd70e663e1caee5c7f983dbfd16",
          "url": "https://github.com/mlrun/storey/commit/fd2b7638b48ee2923ae772c93aba5a2cef7605dc"
        },
        "date": 1607599530508,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1104.8475373928513,
            "unit": "iter/sec",
            "range": "stddev: 0.00011206993866290668",
            "extra": "mean: 905.1022572397058 usec\nrounds: 587"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 757.7694807773489,
            "unit": "iter/sec",
            "range": "stddev: 0.00017424215429433741",
            "extra": "mean: 1.3196625429862414 msec\nrounds: 663"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.622567643158123,
            "unit": "iter/sec",
            "range": "stddev: 0.007055861554458076",
            "extra": "mean: 216.3299873999904 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7227311492771297,
            "unit": "iter/sec",
            "range": "stddev: 0.019214106222301276",
            "extra": "mean: 1.3836403772000039 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3453.996005720628,
            "unit": "iter/sec",
            "range": "stddev: 0.00006920625913863121",
            "extra": "mean: 289.519732606455 usec\nrounds: 2386"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2754.2711569783833,
            "unit": "iter/sec",
            "range": "stddev: 0.00010917142100785037",
            "extra": "mean: 363.07245837663487 usec\nrounds: 1922"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 45.17329725252063,
            "unit": "iter/sec",
            "range": "stddev: 0.0012374331458431824",
            "extra": "mean: 22.136971636362027 msec\nrounds: 44"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 8.927716456239665,
            "unit": "iter/sec",
            "range": "stddev: 0.006703678970422196",
            "extra": "mean: 112.01072579999902 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1093.9727691987935,
            "unit": "iter/sec",
            "range": "stddev: 0.00019926502925653433",
            "extra": "mean: 914.0995353407037 usec\nrounds: 764"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 708.8916058203672,
            "unit": "iter/sec",
            "range": "stddev: 0.00035006648105894437",
            "extra": "mean: 1.4106529006543203 msec\nrounds: 765"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.151866398868503,
            "unit": "iter/sec",
            "range": "stddev: 0.006887633940765469",
            "extra": "mean: 317.272331200013 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.638716818361567,
            "unit": "iter/sec",
            "range": "stddev: 0.028486523460176844",
            "extra": "mean: 1.565639061400003 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1060.4886696048127,
            "unit": "iter/sec",
            "range": "stddev: 0.00014555443301790178",
            "extra": "mean: 942.9615126134695 usec\nrounds: 991"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 392.0195837609679,
            "unit": "iter/sec",
            "range": "stddev: 0.00030140722592413873",
            "extra": "mean: 2.5508929691883586 msec\nrounds: 357"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.8044405819018972,
            "unit": "iter/sec",
            "range": "stddev: 0.02999748681747551",
            "extra": "mean: 1.2430998913999987 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.15011033736356766,
            "unit": "iter/sec",
            "range": "stddev: 0.10029338389010925",
            "extra": "mean: 6.661766388400002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1099.7701240737263,
            "unit": "iter/sec",
            "range": "stddev: 0.00010047745588350352",
            "extra": "mean: 909.2809289052502 usec\nrounds: 858"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 677.4270104256194,
            "unit": "iter/sec",
            "range": "stddev: 0.00038318236838529604",
            "extra": "mean: 1.4761737937961934 msec\nrounds: 548"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.336744283613882,
            "unit": "iter/sec",
            "range": "stddev: 0.005437225762319009",
            "extra": "mean: 230.58772540000518 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6678066559021627,
            "unit": "iter/sec",
            "range": "stddev: 0.021791828718864634",
            "extra": "mean: 1.4974394028000007 sec\nrounds: 5"
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
          "id": "c61fc8545d222cdbeabc8551ab86a669190a67eb",
          "message": "fix loading aggregates when new times exceeds the stored window by more than a window (#92)\n\n* fix loading aggregates when new times exceeds the stored window\r\n\r\n* minor refactor in initialize_from_data\r\n\r\n* remove redundant conndition",
          "timestamp": "2020-12-10T14:03:18+02:00",
          "tree_id": "c891def013dd8c0cbc088ae6b65fb8d069f022cd",
          "url": "https://github.com/mlrun/storey/commit/c61fc8545d222cdbeabc8551ab86a669190a67eb"
        },
        "date": 1607602021675,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1267.1093695983398,
            "unit": "iter/sec",
            "range": "stddev: 0.000054510831516800895",
            "extra": "mean: 789.1978577326671 usec\nrounds: 485"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 900.4315818770912,
            "unit": "iter/sec",
            "range": "stddev: 0.00009829546793973182",
            "extra": "mean: 1.1105785493611215 msec\nrounds: 861"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.276937646040103,
            "unit": "iter/sec",
            "range": "stddev: 0.0010871048034899722",
            "extra": "mean: 159.31335571428917 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0255002357128766,
            "unit": "iter/sec",
            "range": "stddev: 0.014931814819438697",
            "extra": "mean: 975.1338567999937 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4571.824825932539,
            "unit": "iter/sec",
            "range": "stddev: 0.000013380624589342615",
            "extra": "mean: 218.73104024628168 usec\nrounds: 2112"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3701.960780907369,
            "unit": "iter/sec",
            "range": "stddev: 0.00005016079506995765",
            "extra": "mean: 270.12711889262505 usec\nrounds: 2456"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 50.69384585714245,
            "unit": "iter/sec",
            "range": "stddev: 0.00009439346238611628",
            "extra": "mean: 19.726260320001074 msec\nrounds: 50"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.054244394289878,
            "unit": "iter/sec",
            "range": "stddev: 0.007332738653734633",
            "extra": "mean: 99.46048263636116 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1258.3972890249838,
            "unit": "iter/sec",
            "range": "stddev: 0.00010820742135417019",
            "extra": "mean: 794.6615975109164 usec\nrounds: 723"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 851.6256112294678,
            "unit": "iter/sec",
            "range": "stddev: 0.00006721200534717445",
            "extra": "mean: 1.1742249021331428 msec\nrounds: 797"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.563820012716156,
            "unit": "iter/sec",
            "range": "stddev: 0.0016631534520954781",
            "extra": "mean: 219.1146884000034 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9238405746420215,
            "unit": "iter/sec",
            "range": "stddev: 0.0057539714855589464",
            "extra": "mean: 1.082437844200001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1222.7167866463471,
            "unit": "iter/sec",
            "range": "stddev: 0.00005098798188778237",
            "extra": "mean: 817.8508800413119 usec\nrounds: 967"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 450.75056758919834,
            "unit": "iter/sec",
            "range": "stddev: 0.00004023050098144234",
            "extra": "mean: 2.2185218875006996 msec\nrounds: 400"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.9494159259018405,
            "unit": "iter/sec",
            "range": "stddev: 0.006681410584894289",
            "extra": "mean: 1.0532791505999968 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.17900572201889933,
            "unit": "iter/sec",
            "range": "stddev: 0.0167510376576995",
            "extra": "mean: 5.586413600200001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1278.298378065361,
            "unit": "iter/sec",
            "range": "stddev: 0.000056115941386297434",
            "extra": "mean: 782.2899701347104 usec\nrounds: 1038"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 881.8045761639164,
            "unit": "iter/sec",
            "range": "stddev: 0.00006708256062447281",
            "extra": "mean: 1.1340381157356487 msec\nrounds: 769"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.846596919857328,
            "unit": "iter/sec",
            "range": "stddev: 0.0020870727966802727",
            "extra": "mean: 171.039668666675 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9639368370462825,
            "unit": "iter/sec",
            "range": "stddev: 0.01706659145553631",
            "extra": "mean: 1.0374123714000008 sec\nrounds: 5"
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
          "id": "4d6312f645fbe6d3d529df8403e53bd38e7be5d1",
          "message": "Lock grpcio-tools version to avoid segfaults. Update v3io-py. (#96)\n\n* Lock grpcio-tools version to avoid segfaults. Update v3io-py.\r\n\r\n* Use latest grpcio-tools that doesn't segfault.\r\n\r\n* Use grpcio 1.30.0 as in frames Pipfile.lock.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-10T14:20:56+02:00",
          "tree_id": "68a8596092e1216c40a14f280f6d3ba55ecf4065",
          "url": "https://github.com/mlrun/storey/commit/4d6312f645fbe6d3d529df8403e53bd38e7be5d1"
        },
        "date": 1607603063497,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1367.9938199836483,
            "unit": "iter/sec",
            "range": "stddev: 0.00006681845667137136",
            "extra": "mean: 730.9974543685827 usec\nrounds: 515"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 951.8069698794804,
            "unit": "iter/sec",
            "range": "stddev: 0.00006764834134228214",
            "extra": "mean: 1.0506331973242662 msec\nrounds: 897"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.536171577820751,
            "unit": "iter/sec",
            "range": "stddev: 0.0013977581711263896",
            "extra": "mean: 152.9947597142812 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0645322287910362,
            "unit": "iter/sec",
            "range": "stddev: 0.013154077195395217",
            "extra": "mean: 939.3797322000069 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5305.814145005231,
            "unit": "iter/sec",
            "range": "stddev: 0.000018808937789400927",
            "extra": "mean: 188.47248936176487 usec\nrounds: 2350"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4217.567467192775,
            "unit": "iter/sec",
            "range": "stddev: 0.000022355350942041857",
            "extra": "mean: 237.1034981132389 usec\nrounds: 2385"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 57.00559542775133,
            "unit": "iter/sec",
            "range": "stddev: 0.0006319854506207795",
            "extra": "mean: 17.54213761818164 msec\nrounds: 55"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.127726594096165,
            "unit": "iter/sec",
            "range": "stddev: 0.0017043839608748214",
            "extra": "mean: 89.86561554545668 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1344.9877825139229,
            "unit": "iter/sec",
            "range": "stddev: 0.00008343842598094965",
            "extra": "mean: 743.5011774834827 usec\nrounds: 755"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 879.3947758764773,
            "unit": "iter/sec",
            "range": "stddev: 0.00009541898706856827",
            "extra": "mean: 1.1371457136566652 msec\nrounds: 908"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.890409488471927,
            "unit": "iter/sec",
            "range": "stddev: 0.0018045781849866287",
            "extra": "mean: 204.48185419999731 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9859410061670508,
            "unit": "iter/sec",
            "range": "stddev: 0.00648926709895321",
            "extra": "mean: 1.0142594675999987 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1311.0573189549384,
            "unit": "iter/sec",
            "range": "stddev: 0.00006363089521425946",
            "extra": "mean: 762.7431581687928 usec\nrounds: 961"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 467.616451235386,
            "unit": "iter/sec",
            "range": "stddev: 0.0010643022162285633",
            "extra": "mean: 2.1385047454128725 msec\nrounds: 436"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.0061431014847895,
            "unit": "iter/sec",
            "range": "stddev: 0.014472085920546872",
            "extra": "mean: 993.8944058000061 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.19005601913251488,
            "unit": "iter/sec",
            "range": "stddev: 0.05176613818367887",
            "extra": "mean: 5.261606575600001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1390.4741455705107,
            "unit": "iter/sec",
            "range": "stddev: 0.0000554492997958865",
            "extra": "mean: 719.17913985355 usec\nrounds: 1094"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 925.6685281914276,
            "unit": "iter/sec",
            "range": "stddev: 0.00007127676718505145",
            "extra": "mean: 1.0803003122011736 msec\nrounds: 836"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.279224548240919,
            "unit": "iter/sec",
            "range": "stddev: 0.00063411176674983",
            "extra": "mean: 159.25533357142692 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0549931771766008,
            "unit": "iter/sec",
            "range": "stddev: 0.0028533861161613486",
            "extra": "mean: 947.8734286000076 msec\nrounds: 5"
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
          "id": "2ea9832a2a75747b39487f00c6827495a788ac86",
          "message": "add benchmark tests (#98)\n\n* add benchmark tests\r\n\r\n* rename test",
          "timestamp": "2020-12-13T12:26:50+02:00",
          "tree_id": "0d468c34801e62e8e30fb6406ca18f4de32fff4b",
          "url": "https://github.com/mlrun/storey/commit/2ea9832a2a75747b39487f00c6827495a788ac86"
        },
        "date": 1607855754285,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1072.6992588493226,
            "unit": "iter/sec",
            "range": "stddev: 0.00010126322353392654",
            "extra": "mean: 932.2277346147264 usec\nrounds: 520"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 761.6551581718411,
            "unit": "iter/sec",
            "range": "stddev: 0.00022748938821100516",
            "extra": "mean: 1.3129301223407255 msec\nrounds: 752"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.633383867519751,
            "unit": "iter/sec",
            "range": "stddev: 0.005227274651193495",
            "extra": "mean: 215.8249842000032 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7103138870304624,
            "unit": "iter/sec",
            "range": "stddev: 0.03833692621438924",
            "extra": "mean: 1.4078283111999952 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3329.3143797611765,
            "unit": "iter/sec",
            "range": "stddev: 0.00006836418484261163",
            "extra": "mean: 300.362142451604 usec\nrounds: 2113"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2764.010684518345,
            "unit": "iter/sec",
            "range": "stddev: 0.00004329379545628364",
            "extra": "mean: 361.7931021761804 usec\nrounds: 2114"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 45.19614990645718,
            "unit": "iter/sec",
            "range": "stddev: 0.0012979200476030675",
            "extra": "mean: 22.12577845833567 msec\nrounds: 48"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.237834633677727,
            "unit": "iter/sec",
            "range": "stddev: 0.0018296248960408916",
            "extra": "mean: 108.25047639999639 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1058.5643388073033,
            "unit": "iter/sec",
            "range": "stddev: 0.00035610427687788525",
            "extra": "mean: 944.67569267137 usec\nrounds: 846"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 693.3256849294262,
            "unit": "iter/sec",
            "range": "stddev: 0.0003670881893656768",
            "extra": "mean: 1.442323603086752 msec\nrounds: 713"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.1267377783224335,
            "unit": "iter/sec",
            "range": "stddev: 0.014217518934443419",
            "extra": "mean: 319.8221504000003 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6173217980832373,
            "unit": "iter/sec",
            "range": "stddev: 0.035272347155489124",
            "extra": "mean: 1.6199006791999977 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 997.9428502882342,
            "unit": "iter/sec",
            "range": "stddev: 0.00030669177060974853",
            "extra": "mean: 1.0020613903002278 msec\nrounds: 866"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 379.9668947886547,
            "unit": "iter/sec",
            "range": "stddev: 0.001395280445098842",
            "extra": "mean: 2.6318082278094788 msec\nrounds: 338"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7751024361040173,
            "unit": "iter/sec",
            "range": "stddev: 0.025866399095525443",
            "extra": "mean: 1.2901520539999978 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.14930881403005275,
            "unit": "iter/sec",
            "range": "stddev: 0.09288909952330553",
            "extra": "mean: 6.697528250400012 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1026.7553169325233,
            "unit": "iter/sec",
            "range": "stddev: 0.0003911174993927676",
            "extra": "mean: 973.9418764224606 usec\nrounds: 615"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 706.9735858309535,
            "unit": "iter/sec",
            "range": "stddev: 0.00025578989846316875",
            "extra": "mean: 1.4144800032728704 msec\nrounds: 611"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.18568763115962,
            "unit": "iter/sec",
            "range": "stddev: 0.01758011195285658",
            "extra": "mean: 238.909371200009 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6778793153108827,
            "unit": "iter/sec",
            "range": "stddev: 0.024101158769879404",
            "extra": "mean: 1.4751888387999998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.026032233369107968,
            "unit": "iter/sec",
            "range": "stddev: 0.3851720920170321",
            "extra": "mean: 38.413915003800014 sec\nrounds: 5"
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
          "id": "8aaf9081e81b30362532c32004985f51b0082af2",
          "message": "Lock dependency versions. (#97)\n\n* Lock dependency versions.\r\n\r\nWe don't use a Pipfile.lock to avoid targeting a specific version of Python.\r\n\r\n* Don't lock dev and internal dependencies for now.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-13T12:56:40+02:00",
          "tree_id": "6af5a65ccfa217675a7040ae2896f6abed26cc29",
          "url": "https://github.com/mlrun/storey/commit/8aaf9081e81b30362532c32004985f51b0082af2"
        },
        "date": 1607857535665,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 938.6591697941451,
            "unit": "iter/sec",
            "range": "stddev: 0.0005286159493594781",
            "extra": "mean: 1.0653494177437242 msec\nrounds: 541"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 713.0720794391807,
            "unit": "iter/sec",
            "range": "stddev: 0.0004866539742325903",
            "extra": "mean: 1.402382772841819 msec\nrounds: 788"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.355872523609972,
            "unit": "iter/sec",
            "range": "stddev: 0.023257980406967218",
            "extra": "mean: 229.57512979999706 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.6525198223691819,
            "unit": "iter/sec",
            "range": "stddev: 0.09054596431064606",
            "extra": "mean: 1.5325204932000076 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3341.8609365830634,
            "unit": "iter/sec",
            "range": "stddev: 0.00011057330051720127",
            "extra": "mean: 299.2344741377734 usec\nrounds: 2320"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2723.2825967344056,
            "unit": "iter/sec",
            "range": "stddev: 0.0001068259964714035",
            "extra": "mean: 367.2039035534318 usec\nrounds: 2167"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 44.260200362448835,
            "unit": "iter/sec",
            "range": "stddev: 0.002374847501483911",
            "extra": "mean: 22.59366184090794 msec\nrounds: 44"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.362593028186934,
            "unit": "iter/sec",
            "range": "stddev: 0.0034447220208248556",
            "extra": "mean: 106.80801750000342 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 711.6162874256088,
            "unit": "iter/sec",
            "range": "stddev: 0.001451292509402479",
            "extra": "mean: 1.4052517032987928 msec\nrounds: 91"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 668.4334576074046,
            "unit": "iter/sec",
            "range": "stddev: 0.0006257486182530473",
            "extra": "mean: 1.4960352277688296 msec\nrounds: 641"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.8110881873553843,
            "unit": "iter/sec",
            "range": "stddev: 0.03397871082776914",
            "extra": "mean: 355.7341261999966 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.565891645560799,
            "unit": "iter/sec",
            "range": "stddev: 0.052925971446770095",
            "extra": "mean: 1.7671227484000043 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1011.4810461751142,
            "unit": "iter/sec",
            "range": "stddev: 0.0002366258156236304",
            "extra": "mean: 988.649272056526 usec\nrounds: 136"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 362.37516495520794,
            "unit": "iter/sec",
            "range": "stddev: 0.001501123069178516",
            "extra": "mean: 2.7595710101259474 msec\nrounds: 395"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 0.7954334824301622,
            "unit": "iter/sec",
            "range": "stddev: 0.02071077745396778",
            "extra": "mean: 1.257176146200004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.13833464990362193,
            "unit": "iter/sec",
            "range": "stddev: 0.1625723833313694",
            "extra": "mean: 7.228846862999996 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 824.4618995787968,
            "unit": "iter/sec",
            "range": "stddev: 0.0010839040617139323",
            "extra": "mean: 1.2129123256161172 msec\nrounds: 648"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 640.0331256625243,
            "unit": "iter/sec",
            "range": "stddev: 0.0008848639343467191",
            "extra": "mean: 1.5624191309861646 msec\nrounds: 710"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 3.4051691981685988,
            "unit": "iter/sec",
            "range": "stddev: 0.024460560836106045",
            "extra": "mean: 293.67116339999484 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.5723347810072653,
            "unit": "iter/sec",
            "range": "stddev: 0.12828983495946894",
            "extra": "mean: 1.74722912740001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.024977519867946597,
            "unit": "iter/sec",
            "range": "stddev: 0.17347239447504217",
            "extra": "mean: 40.03600058320002 sec\nrounds: 5"
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
          "id": "74473d72b2104267c7695890e847baf94a854050",
          "message": "optimize aggregations (#94)\n\n* optimize aggregations - save an accumulative and sliding aggregations per window\r\n\r\n* always recalculate aggregations when aggr max_value provided\r\n\r\n* optimize type checks\r\n\r\n* minor refactoring\r\n\r\n* fix break",
          "timestamp": "2020-12-13T13:16:24+02:00",
          "tree_id": "ca502aff84142697d9c7cf32485699b4784c5593",
          "url": "https://github.com/mlrun/storey/commit/74473d72b2104267c7695890e847baf94a854050"
        },
        "date": 1607858564117,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1084.4452887980779,
            "unit": "iter/sec",
            "range": "stddev: 0.0007214073937487936",
            "extra": "mean: 922.1304295658189 usec\nrounds: 575"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 795.7996199199154,
            "unit": "iter/sec",
            "range": "stddev: 0.0003263455903096611",
            "extra": "mean: 1.256597735119092 msec\nrounds: 672"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 5.060585604124257,
            "unit": "iter/sec",
            "range": "stddev: 0.013740088119054278",
            "extra": "mean: 197.60558919999767 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7323560922940733,
            "unit": "iter/sec",
            "range": "stddev: 0.02712991110224297",
            "extra": "mean: 1.3654559722000044 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3813.6758389692404,
            "unit": "iter/sec",
            "range": "stddev: 0.00007909518345640092",
            "extra": "mean: 262.2142107049874 usec\nrounds: 2653"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3115.42514569849,
            "unit": "iter/sec",
            "range": "stddev: 0.000141229766594321",
            "extra": "mean: 320.9834784124772 usec\nrounds: 2872"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 49.27645514692937,
            "unit": "iter/sec",
            "range": "stddev: 0.0036312991703560233",
            "extra": "mean: 20.29366757446866 msec\nrounds: 47"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.277930829252941,
            "unit": "iter/sec",
            "range": "stddev: 0.009306123367240179",
            "extra": "mean: 97.29584841666868 msec\nrounds: 12"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1068.8883364255366,
            "unit": "iter/sec",
            "range": "stddev: 0.0006882187893212594",
            "extra": "mean: 935.5514190978023 usec\nrounds: 754"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 740.1954270998643,
            "unit": "iter/sec",
            "range": "stddev: 0.0006709298501306473",
            "extra": "mean: 1.3509945662837548 msec\nrounds: 611"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.09623002604758,
            "unit": "iter/sec",
            "range": "stddev: 0.014648800152551655",
            "extra": "mean: 322.9734197999903 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6551194432249797,
            "unit": "iter/sec",
            "range": "stddev: 0.060928058400839834",
            "extra": "mean: 1.5264392018000024 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1089.9300585026278,
            "unit": "iter/sec",
            "range": "stddev: 0.00044822033348832353",
            "extra": "mean: 917.4900647972074 usec\nrounds: 1034"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 391.81460324155574,
            "unit": "iter/sec",
            "range": "stddev: 0.0011474427767262873",
            "extra": "mean: 2.5522274864867525 msec\nrounds: 407"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.4699592176954768,
            "unit": "iter/sec",
            "range": "stddev: 0.06927427111035958",
            "extra": "mean: 680.2909822000004 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.2825648534767521,
            "unit": "iter/sec",
            "range": "stddev: 0.03566711956282683",
            "extra": "mean: 3.5390105587999985 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1195.788067645699,
            "unit": "iter/sec",
            "range": "stddev: 0.00021440337191319916",
            "extra": "mean: 836.2685889388645 usec\nrounds: 669"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 725.3720447399522,
            "unit": "iter/sec",
            "range": "stddev: 0.000798216288036659",
            "extra": "mean: 1.378602893854977 msec\nrounds: 537"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.580540112720089,
            "unit": "iter/sec",
            "range": "stddev: 0.007890857694076124",
            "extra": "mean: 218.31486579999932 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.6861847076003276,
            "unit": "iter/sec",
            "range": "stddev: 0.05786602508560269",
            "extra": "mean: 1.4573335559999918 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.04169218437958153,
            "unit": "iter/sec",
            "range": "stddev: 0.22657092456158034",
            "extra": "mean: 23.985310793400004 sec\nrounds: 5"
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
          "id": "7b581020b6039f93af19efbbd32e5ccc88cc062a",
          "message": "Fix nontermination on error when awaiting result. (#99)\n\n* Fix nontermination on error when awaiting result.\r\n\r\n* Fix nontermination when _ConcurrentJobExecution and Complete are combined.\r\n\r\n* Fix nontermination in _ConcurrentByKeyJobExecution+Complete.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-14T12:33:03+02:00",
          "tree_id": "c3caa6f7b945f8fda07c6ed666f0150ed377ce0f",
          "url": "https://github.com/mlrun/storey/commit/7b581020b6039f93af19efbbd32e5ccc88cc062a"
        },
        "date": 1607942415914,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1138.4763642876053,
            "unit": "iter/sec",
            "range": "stddev: 0.00023655975999716634",
            "extra": "mean: 878.3669396823569 usec\nrounds: 630"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 776.2067025560776,
            "unit": "iter/sec",
            "range": "stddev: 0.00042161867428594215",
            "extra": "mean: 1.2883166258510301 msec\nrounds: 882"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.869875739301121,
            "unit": "iter/sec",
            "range": "stddev: 0.009999753691721765",
            "extra": "mean: 205.34404849999532 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.7568236024282539,
            "unit": "iter/sec",
            "range": "stddev: 0.03160511479584323",
            "extra": "mean: 1.3213118576 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3630.556129797035,
            "unit": "iter/sec",
            "range": "stddev: 0.0001369329822560694",
            "extra": "mean: 275.4398952250615 usec\nrounds: 2157"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2974.1385913258414,
            "unit": "iter/sec",
            "range": "stddev: 0.00012237150880576466",
            "extra": "mean: 336.23180941080824 usec\nrounds: 2104"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 49.2435877412439,
            "unit": "iter/sec",
            "range": "stddev: 0.0015051137975136614",
            "extra": "mean: 20.307212489362374 msec\nrounds: 47"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 9.687899015631062,
            "unit": "iter/sec",
            "range": "stddev: 0.004949723014194737",
            "extra": "mean: 103.22155488889153 msec\nrounds: 9"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1135.8166090354318,
            "unit": "iter/sec",
            "range": "stddev: 0.00029347204858438735",
            "extra": "mean: 880.4238219841043 usec\nrounds: 1028"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 727.8062116578825,
            "unit": "iter/sec",
            "range": "stddev: 0.0006780659937206709",
            "extra": "mean: 1.3739921204053516 msec\nrounds: 789"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 3.3434333394791094,
            "unit": "iter/sec",
            "range": "stddev: 0.0099401578170586",
            "extra": "mean: 299.0937453999891 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6413018572996527,
            "unit": "iter/sec",
            "range": "stddev: 0.024590165641013965",
            "extra": "mean: 1.559328089600001 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1054.4563373926946,
            "unit": "iter/sec",
            "range": "stddev: 0.0005388121036433371",
            "extra": "mean: 948.3560054014695 usec\nrounds: 1111"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 394.1424296775343,
            "unit": "iter/sec",
            "range": "stddev: 0.0011686269158145742",
            "extra": "mean: 2.53715389337338 msec\nrounds: 347"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.4917999007106364,
            "unit": "iter/sec",
            "range": "stddev: 0.009855006169685276",
            "extra": "mean: 670.331188199998 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.2767413009356838,
            "unit": "iter/sec",
            "range": "stddev: 0.03727268071943024",
            "extra": "mean: 3.6134830494000085 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1107.742688281846,
            "unit": "iter/sec",
            "range": "stddev: 0.00042575185191633314",
            "extra": "mean: 902.7367190760164 usec\nrounds: 865"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 738.6495822593849,
            "unit": "iter/sec",
            "range": "stddev: 0.0006180141315536497",
            "extra": "mean: 1.3538219258734232 msec\nrounds: 715"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.40419034435395,
            "unit": "iter/sec",
            "range": "stddev: 0.006347560224476054",
            "extra": "mean: 227.05648979998614 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7233616446949163,
            "unit": "iter/sec",
            "range": "stddev: 0.027257685878713155",
            "extra": "mean: 1.3824343706000035 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.041221695488769194,
            "unit": "iter/sec",
            "range": "stddev: 0.07663998480215911",
            "extra": "mean: 24.259070087799977 sec\nrounds: 5"
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
          "id": "7b581020b6039f93af19efbbd32e5ccc88cc062a",
          "message": "Fix nontermination on error when awaiting result. (#99)\n\n* Fix nontermination on error when awaiting result.\r\n\r\n* Fix nontermination when _ConcurrentJobExecution and Complete are combined.\r\n\r\n* Fix nontermination in _ConcurrentByKeyJobExecution+Complete.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-14T12:33:03+02:00",
          "tree_id": "c3caa6f7b945f8fda07c6ed666f0150ed377ce0f",
          "url": "https://github.com/mlrun/storey/commit/7b581020b6039f93af19efbbd32e5ccc88cc062a"
        },
        "date": 1607955995795,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1590.3879673114423,
            "unit": "iter/sec",
            "range": "stddev: 0.000025282382814866195",
            "extra": "mean: 628.7773930347978 usec\nrounds: 603"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 1119.1385834844998,
            "unit": "iter/sec",
            "range": "stddev: 0.000043932279666028846",
            "extra": "mean: 893.5443874041449 usec\nrounds: 1048"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 7.353412416661099,
            "unit": "iter/sec",
            "range": "stddev: 0.000955396411757455",
            "extra": "mean: 135.9912845000011 msec\nrounds: 8"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.1641955351340936,
            "unit": "iter/sec",
            "range": "stddev: 0.01652897157990949",
            "extra": "mean: 858.9622360000021 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5558.189470378951,
            "unit": "iter/sec",
            "range": "stddev: 0.000009374218672570494",
            "extra": "mean: 179.91470160009158 usec\nrounds: 2500"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4489.808591738135,
            "unit": "iter/sec",
            "range": "stddev: 0.00000931243269491716",
            "extra": "mean: 222.7266440355915 usec\nrounds: 2666"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 60.871531808968236,
            "unit": "iter/sec",
            "range": "stddev: 0.00009104386476723128",
            "extra": "mean: 16.428040666666277 msec\nrounds: 60"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 12.011912265136766,
            "unit": "iter/sec",
            "range": "stddev: 0.006791666387375434",
            "extra": "mean: 83.25069130769364 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1578.2018066532814,
            "unit": "iter/sec",
            "range": "stddev: 0.000035964604458850225",
            "extra": "mean: 633.6325277187394 usec\nrounds: 938"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 1033.0853676021259,
            "unit": "iter/sec",
            "range": "stddev: 0.00005662141292823754",
            "extra": "mean: 967.9742171947322 usec\nrounds: 884"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 5.142733094257552,
            "unit": "iter/sec",
            "range": "stddev: 0.0012049073888908818",
            "extra": "mean: 194.44913466666472 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 1.0502802678671306,
            "unit": "iter/sec",
            "range": "stddev: 0.010000563600923261",
            "extra": "mean: 952.1268090000035 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1519.5822885203097,
            "unit": "iter/sec",
            "range": "stddev: 0.00007211180608027176",
            "extra": "mean: 658.0755827140813 usec\nrounds: 1076"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 514.3695112669152,
            "unit": "iter/sec",
            "range": "stddev: 0.0012021705179053595",
            "extra": "mean: 1.9441276710529658 msec\nrounds: 456"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 2.1785502800300898,
            "unit": "iter/sec",
            "range": "stddev: 0.0063148774374788205",
            "extra": "mean: 459.020849399991 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.4026849568762011,
            "unit": "iter/sec",
            "range": "stddev: 0.022806526348141655",
            "extra": "mean: 2.4833309089999944 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1594.546953474862,
            "unit": "iter/sec",
            "range": "stddev: 0.000038158841453901894",
            "extra": "mean: 627.1373808220474 usec\nrounds: 1095"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 1089.6093012127585,
            "unit": "iter/sec",
            "range": "stddev: 0.00003887569032210587",
            "extra": "mean: 917.7601539257957 usec\nrounds: 968"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.804568611473305,
            "unit": "iter/sec",
            "range": "stddev: 0.0011214527978002729",
            "extra": "mean: 146.96008771428686 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.09297034395037,
            "unit": "iter/sec",
            "range": "stddev: 0.015534287009284976",
            "extra": "mean: 914.9379080000074 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.05446269461532351,
            "unit": "iter/sec",
            "range": "stddev: 0.037540530280321185",
            "extra": "mean: 18.361192134600003 sec\nrounds: 5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "galt@iguazio.com",
            "name": "Gal Topper"
          },
          "committer": {
            "email": "galt@iguazio.com",
            "name": "Gal Topper"
          },
          "distinct": true,
          "id": "767a9cc001e8a8f461271ba3d3a6f181ce6ad998",
          "message": "Release actions.",
          "timestamp": "2020-12-14T17:10:17+02:00",
          "tree_id": "1342511be8be6bbe8269a6cead7c43d3fbabdc6b",
          "url": "https://github.com/mlrun/storey/commit/767a9cc001e8a8f461271ba3d3a6f181ce6ad998"
        },
        "date": 1607959030599,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1306.6351539075747,
            "unit": "iter/sec",
            "range": "stddev: 0.00025476835029131716",
            "extra": "mean: 765.3245797110517 usec\nrounds: 552"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 932.3137979454436,
            "unit": "iter/sec",
            "range": "stddev: 0.000632674612528882",
            "extra": "mean: 1.0726002363192713 msec\nrounds: 804"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.519273516894848,
            "unit": "iter/sec",
            "range": "stddev: 0.004861489593774518",
            "extra": "mean: 153.39132457143836 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0523910271728356,
            "unit": "iter/sec",
            "range": "stddev: 0.015054276345309888",
            "extra": "mean: 950.2171475999944 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4898.184611696015,
            "unit": "iter/sec",
            "range": "stddev: 0.000015698917189043943",
            "extra": "mean: 204.15727035117732 usec\nrounds: 2334"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3927.5217793336783,
            "unit": "iter/sec",
            "range": "stddev: 0.00001497733760078674",
            "extra": "mean: 254.61348305232173 usec\nrounds: 2478"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 54.17042399999812,
            "unit": "iter/sec",
            "range": "stddev: 0.00026282750560322225",
            "extra": "mean: 18.460257944446486 msec\nrounds: 54"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.746237939681391,
            "unit": "iter/sec",
            "range": "stddev: 0.006855119052620167",
            "extra": "mean: 93.05582154545598 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1329.617096373451,
            "unit": "iter/sec",
            "range": "stddev: 0.00007094158425728842",
            "extra": "mean: 752.0962258439018 usec\nrounds: 828"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 884.4428266165156,
            "unit": "iter/sec",
            "range": "stddev: 0.0003806091137473689",
            "extra": "mean: 1.1306553345291461 msec\nrounds: 837"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.767076208757916,
            "unit": "iter/sec",
            "range": "stddev: 0.0020322942475176045",
            "extra": "mean: 209.7721866000029 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.9571698435757627,
            "unit": "iter/sec",
            "range": "stddev: 0.008643751374401962",
            "extra": "mean: 1.0447466629999895 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1281.7045177911325,
            "unit": "iter/sec",
            "range": "stddev: 0.00004749774512122274",
            "extra": "mean: 780.2110284540331 usec\nrounds: 984"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 461.0012521745115,
            "unit": "iter/sec",
            "range": "stddev: 0.00016293298959711602",
            "extra": "mean: 2.1691915049754598 msec\nrounds: 402"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.8727415584221008,
            "unit": "iter/sec",
            "range": "stddev: 0.012799958120069356",
            "extra": "mean: 533.9765091999993 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.3473422512578267,
            "unit": "iter/sec",
            "range": "stddev: 0.048166823207687635",
            "extra": "mean: 2.8790047751999963 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1295.2941660375147,
            "unit": "iter/sec",
            "range": "stddev: 0.00021418338949493642",
            "extra": "mean: 772.0254025841398 usec\nrounds: 1006"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 918.9146549682234,
            "unit": "iter/sec",
            "range": "stddev: 0.00023838529083552876",
            "extra": "mean: 1.088240343750509 msec\nrounds: 768"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.75630339037941,
            "unit": "iter/sec",
            "range": "stddev: 0.007376437397420709",
            "extra": "mean: 173.7226015000033 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0222120124049985,
            "unit": "iter/sec",
            "range": "stddev: 0.0057667340315037535",
            "extra": "mean: 978.2706403999896 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.047388182013820626,
            "unit": "iter/sec",
            "range": "stddev: 0.04199098338068542",
            "extra": "mean: 21.102307738000015 sec\nrounds: 5"
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
          "id": "7a688d36c2d9fd68ccda007e43ad1a535a249f28",
          "message": "Fix certain cases of branch nesting in build_flow. (#102)\n\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-15T14:45:45+02:00",
          "tree_id": "85e9b681c32210212a6fbc6da4cfe890b0592bd1",
          "url": "https://github.com/mlrun/storey/commit/7a688d36c2d9fd68ccda007e43ad1a535a249f28"
        },
        "date": 1608036809525,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 988.5396195714252,
            "unit": "iter/sec",
            "range": "stddev: 0.000664724873944022",
            "extra": "mean: 1.0115932434084365 msec\nrounds: 493"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 737.3110833401898,
            "unit": "iter/sec",
            "range": "stddev: 0.0010804070021350185",
            "extra": "mean: 1.356279625514062 msec\nrounds: 729"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 4.626126859158891,
            "unit": "iter/sec",
            "range": "stddev: 0.024884151898445565",
            "extra": "mean: 216.16354899999806 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.71511944763349,
            "unit": "iter/sec",
            "range": "stddev: 0.03421006428037412",
            "extra": "mean: 1.398367787799998 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 3459.737065468759,
            "unit": "iter/sec",
            "range": "stddev: 0.000047247929559128974",
            "extra": "mean: 289.03930590011765 usec\nrounds: 1932"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 2726.6654496810124,
            "unit": "iter/sec",
            "range": "stddev: 0.00005101387181263265",
            "extra": "mean: 366.7483299489448 usec\nrounds: 2167"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 44.709704703375984,
            "unit": "iter/sec",
            "range": "stddev: 0.0024233750265321087",
            "extra": "mean: 22.366508717390186 msec\nrounds: 46"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 8.958032052507107,
            "unit": "iter/sec",
            "range": "stddev: 0.01264446669735608",
            "extra": "mean: 111.63166129999809 msec\nrounds: 10"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1058.4226822324854,
            "unit": "iter/sec",
            "range": "stddev: 0.0005072107965304154",
            "extra": "mean: 944.8021256410937 usec\nrounds: 780"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 639.2413135046035,
            "unit": "iter/sec",
            "range": "stddev: 0.0006618974161475623",
            "extra": "mean: 1.5643544603172752 msec\nrounds: 252"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 2.9964249109039316,
            "unit": "iter/sec",
            "range": "stddev: 0.016925796426200568",
            "extra": "mean: 333.7310393999928 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.6112213804397815,
            "unit": "iter/sec",
            "range": "stddev: 0.06699105635770436",
            "extra": "mean: 1.636068423000006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 878.7837003240451,
            "unit": "iter/sec",
            "range": "stddev: 0.0024075012354776077",
            "extra": "mean: 1.1379364451471474 msec\nrounds: 948"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 357.3055977303622,
            "unit": "iter/sec",
            "range": "stddev: 0.0016612946191838277",
            "extra": "mean: 2.798724694916876 msec\nrounds: 354"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.432045599017203,
            "unit": "iter/sec",
            "range": "stddev: 0.03170222024286048",
            "extra": "mean: 698.3017864000203 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.26768053097599304,
            "unit": "iter/sec",
            "range": "stddev: 0.06466318752206797",
            "extra": "mean: 3.73579653459999 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 862.8396141054336,
            "unit": "iter/sec",
            "range": "stddev: 0.003870330243453794",
            "extra": "mean: 1.1589639414467197 msec\nrounds: 871"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 743.9223420396967,
            "unit": "iter/sec",
            "range": "stddev: 0.00020540521314521188",
            "extra": "mean: 1.344226330477165 msec\nrounds: 817"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 4.173725779508682,
            "unit": "iter/sec",
            "range": "stddev: 0.030452948192083922",
            "extra": "mean: 239.5940827999766 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.7031522172424725,
            "unit": "iter/sec",
            "range": "stddev: 0.031004602447983177",
            "extra": "mean: 1.422167171600006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.037043313649790785,
            "unit": "iter/sec",
            "range": "stddev: 0.18264860154331397",
            "extra": "mean: 26.99542512459999 sec\nrounds: 5"
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
          "id": "468e4c6f6cb53127c4af69b4380c06470ddeec93",
          "message": "Deep copy event body when branching. (#103)\n\n* Deep copy event body when branching.\r\n\r\n* Deep copy whole event except for AwaitableResult.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-16T11:53:41+02:00",
          "tree_id": "f4983151ccc8e3552ed0e8852a084e2c70f0636d",
          "url": "https://github.com/mlrun/storey/commit/468e4c6f6cb53127c4af69b4380c06470ddeec93"
        },
        "date": 1608112811621,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1268.4884952280827,
            "unit": "iter/sec",
            "range": "stddev: 0.0004388129405602164",
            "extra": "mean: 788.3398263065786 usec\nrounds: 593"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 948.3668589660903,
            "unit": "iter/sec",
            "range": "stddev: 0.00018619325283496347",
            "extra": "mean: 1.0544442696892635 msec\nrounds: 838"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.562893427003233,
            "unit": "iter/sec",
            "range": "stddev: 0.006915912946423574",
            "extra": "mean: 152.3718175714188 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.076055875712127,
            "unit": "iter/sec",
            "range": "stddev: 0.012616854158923966",
            "extra": "mean: 929.3197710000015 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4777.504724383605,
            "unit": "iter/sec",
            "range": "stddev: 0.0001031779098530512",
            "extra": "mean: 209.31428804165554 usec\nrounds: 2333"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3651.6701446994525,
            "unit": "iter/sec",
            "range": "stddev: 0.000060711662808791916",
            "extra": "mean: 273.8472973665326 usec\nrounds: 2734"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 52.242180224195664,
            "unit": "iter/sec",
            "range": "stddev: 0.0020841381271493116",
            "extra": "mean: 19.141620730768345 msec\nrounds: 52"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.785801382785946,
            "unit": "iter/sec",
            "range": "stddev: 0.006573596057528361",
            "extra": "mean: 84.84785781818584 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1296.279582928631,
            "unit": "iter/sec",
            "range": "stddev: 0.00019827423343946054",
            "extra": "mean: 771.4385177160171 usec\nrounds: 1016"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 891.9304117886695,
            "unit": "iter/sec",
            "range": "stddev: 0.00026041288692290183",
            "extra": "mean: 1.1211636993009448 msec\nrounds: 858"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.562394929356443,
            "unit": "iter/sec",
            "range": "stddev: 0.011510897076997076",
            "extra": "mean: 219.1831298000011 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 1.0269411153981343,
            "unit": "iter/sec",
            "range": "stddev: 0.01622346889222725",
            "extra": "mean: 973.7656668 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1288.3185590995367,
            "unit": "iter/sec",
            "range": "stddev: 0.00006181346206349997",
            "extra": "mean: 776.2055377816995 usec\nrounds: 1019"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 454.4267122678043,
            "unit": "iter/sec",
            "range": "stddev: 0.0006903929695225865",
            "extra": "mean: 2.2005748627969663 msec\nrounds: 379"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.9137326105782089,
            "unit": "iter/sec",
            "range": "stddev: 0.02298887720913867",
            "extra": "mean: 522.5390394000044 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.3493868748091187,
            "unit": "iter/sec",
            "range": "stddev: 0.022140820638293115",
            "extra": "mean: 2.862156743999992 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1347.6043120673135,
            "unit": "iter/sec",
            "range": "stddev: 0.0002282205406821016",
            "extra": "mean: 742.0575839995157 usec\nrounds: 1125"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 930.7263380693323,
            "unit": "iter/sec",
            "range": "stddev: 0.00033931900607494245",
            "extra": "mean: 1.074429678303041 msec\nrounds: 802"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.099168637832261,
            "unit": "iter/sec",
            "range": "stddev: 0.0035207545271360896",
            "extra": "mean: 163.95677171428653 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0342632626347492,
            "unit": "iter/sec",
            "range": "stddev: 0.011580407363433498",
            "extra": "mean: 966.871816999992 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.04601065664497899,
            "unit": "iter/sec",
            "range": "stddev: 0.6656456436678561",
            "extra": "mean: 21.7340953796 sec\nrounds: 5"
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
          "id": "f015d8ad7ab56bb00d970fd331abd8f2b1db0c39",
          "message": "Add verbose logging of event progression. (#100)\n\n* Add verbose logging of event progression.\r\n\r\n* Fix.\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-16T12:23:22+02:00",
          "tree_id": "bda9daa1d1ff1eb10be3a31866760fca62e573ba",
          "url": "https://github.com/mlrun/storey/commit/f015d8ad7ab56bb00d970fd331abd8f2b1db0c39"
        },
        "date": 1608114582625,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1289.717851890704,
            "unit": "iter/sec",
            "range": "stddev: 0.00033677941332431867",
            "extra": "mean: 775.3633855141396 usec\nrounds: 428"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 894.1957670603234,
            "unit": "iter/sec",
            "range": "stddev: 0.00031797984692219045",
            "extra": "mean: 1.1183233435419953 msec\nrounds: 751"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.27319543417398,
            "unit": "iter/sec",
            "range": "stddev: 0.003646511456143092",
            "extra": "mean: 159.40839249999783 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 0.9873044125029343,
            "unit": "iter/sec",
            "range": "stddev: 0.038179615393249856",
            "extra": "mean: 1.0128588380000054 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 4491.039338123194,
            "unit": "iter/sec",
            "range": "stddev: 0.00017394510874513798",
            "extra": "mean: 222.66560693674535 usec\nrounds: 2422"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 3795.660144184698,
            "unit": "iter/sec",
            "range": "stddev: 0.00008420585071403041",
            "extra": "mean: 263.4587824023425 usec\nrounds: 2739"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 49.943167272686594,
            "unit": "iter/sec",
            "range": "stddev: 0.0014171129310905453",
            "extra": "mean: 20.022758960000715 msec\nrounds: 50"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 10.143903143682024,
            "unit": "iter/sec",
            "range": "stddev: 0.002558114303105415",
            "extra": "mean: 98.5813829090861 msec\nrounds: 11"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1259.3448885858293,
            "unit": "iter/sec",
            "range": "stddev: 0.00036330673683696756",
            "extra": "mean: 794.0636509216642 usec\nrounds: 868"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 825.2125139680144,
            "unit": "iter/sec",
            "range": "stddev: 0.0003758968277480931",
            "extra": "mean: 1.211809058967761 msec\nrounds: 814"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 4.324509325430722,
            "unit": "iter/sec",
            "range": "stddev: 0.009383621737936016",
            "extra": "mean: 231.2401072 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 0.8861385038582573,
            "unit": "iter/sec",
            "range": "stddev: 0.01952585554482211",
            "extra": "mean: 1.1284917602000006 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1217.3902139777763,
            "unit": "iter/sec",
            "range": "stddev: 0.0003430817697548596",
            "extra": "mean: 821.4293071508583 usec\nrounds: 993"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 415.47084800120814,
            "unit": "iter/sec",
            "range": "stddev: 0.0013421247161269699",
            "extra": "mean: 2.4069077404850607 msec\nrounds: 289"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 1.7978906917712663,
            "unit": "iter/sec",
            "range": "stddev: 0.008190917762440054",
            "extra": "mean: 556.2073403999932 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.3306021975031528,
            "unit": "iter/sec",
            "range": "stddev: 0.0492807572943592",
            "extra": "mean: 3.024783282000004 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1201.4926711343887,
            "unit": "iter/sec",
            "range": "stddev: 0.0006256529595059933",
            "extra": "mean: 832.2980439454952 usec\nrounds: 1024"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 870.4942849737733,
            "unit": "iter/sec",
            "range": "stddev: 0.00021994260798410462",
            "extra": "mean: 1.148772619489545 msec\nrounds: 862"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 5.298094510551607,
            "unit": "iter/sec",
            "range": "stddev: 0.02018959964429676",
            "extra": "mean: 188.74710483333482 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 0.9773136166706643,
            "unit": "iter/sec",
            "range": "stddev: 0.014869249255105773",
            "extra": "mean: 1.023213002400007 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.0450820507117611,
            "unit": "iter/sec",
            "range": "stddev: 0.1320722860342429",
            "extra": "mean: 22.181777097799987 sec\nrounds: 5"
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
          "id": "a611df15fc09718541c5126a7af31303244fcd56",
          "message": "Create Driver base class. (#105)\n\n* Create Driver base class.\r\n\r\n* NoopDriver -> Driver\r\n\r\nCo-authored-by: Gal Topper <galt@iguazio.com>",
          "timestamp": "2020-12-16T18:59:49+02:00",
          "tree_id": "7d801c3349043af179b0f309d52513b44c277008",
          "url": "https://github.com/mlrun/storey/commit/a611df15fc09718541c5126a7af31303244fcd56"
        },
        "date": 1608138345370,
        "tool": "pytest",
        "benches": [
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[0]",
            "value": 1470.9646327245523,
            "unit": "iter/sec",
            "range": "stddev: 0.0002637086977088172",
            "extra": "mean: 679.8259983639297 usec\nrounds: 611"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1]",
            "value": 1060.1855705637101,
            "unit": "iter/sec",
            "range": "stddev: 0.0002903019266771531",
            "extra": "mean: 943.231098182454 usec\nrounds: 550"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[1000]",
            "value": 6.921523632678098,
            "unit": "iter/sec",
            "range": "stddev: 0.001902079946388399",
            "extra": "mean: 144.47685987500947 msec\nrounds: 8"
          },
          {
            "name": "bench/bench_flow.py::test_simple_flow_n_events[5000]",
            "value": 1.0645131505450696,
            "unit": "iter/sec",
            "range": "stddev: 0.03612998419362363",
            "extra": "mean: 939.3965677999972 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[0]",
            "value": 5427.333563120736,
            "unit": "iter/sec",
            "range": "stddev: 0.000023824538130949802",
            "extra": "mean: 184.25254102587286 usec\nrounds: 2340"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1]",
            "value": 4310.006708638363,
            "unit": "iter/sec",
            "range": "stddev: 0.000026708308728167365",
            "extra": "mean: 232.018200341949 usec\nrounds: 2341"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[1000]",
            "value": 59.61918500225378,
            "unit": "iter/sec",
            "range": "stddev: 0.00009893671206366053",
            "extra": "mean: 16.773124288133044 msec\nrounds: 59"
          },
          {
            "name": "bench/bench_flow.py::test_simple_async_flow_n_events[5000]",
            "value": 11.809114844217707,
            "unit": "iter/sec",
            "range": "stddev: 0.006347150236054741",
            "extra": "mean: 84.68035184615438 msec\nrounds: 13"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[0]",
            "value": 1525.0987561258228,
            "unit": "iter/sec",
            "range": "stddev: 0.00007886763511225963",
            "extra": "mean: 655.6952433298677 usec\nrounds: 937"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1]",
            "value": 1006.1992202574537,
            "unit": "iter/sec",
            "range": "stddev: 0.00008519002765371938",
            "extra": "mean: 993.8389733040467 usec\nrounds: 899"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[1000]",
            "value": 5.164594201233809,
            "unit": "iter/sec",
            "range": "stddev: 0.002364358268477415",
            "extra": "mean: 193.62605483333084 msec\nrounds: 6"
          },
          {
            "name": "bench/bench_flow.py::test_complete_flow_n_events[5000]",
            "value": 1.0232753316691936,
            "unit": "iter/sec",
            "range": "stddev: 0.017085468341137423",
            "extra": "mean: 977.254087 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[0]",
            "value": 1484.1540148487181,
            "unit": "iter/sec",
            "range": "stddev: 0.00004202979565831052",
            "extra": "mean: 673.7845196624903 usec\nrounds: 1068"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1]",
            "value": 510.57285838155576,
            "unit": "iter/sec",
            "range": "stddev: 0.001181414454535887",
            "extra": "mean: 1.9585843304907733 msec\nrounds: 469"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[1000]",
            "value": 2.131393629176297,
            "unit": "iter/sec",
            "range": "stddev: 0.003858847382764538",
            "extra": "mean: 469.17659239999807 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_by_key_n_events[5000]",
            "value": 0.3944284613577983,
            "unit": "iter/sec",
            "range": "stddev: 0.007935555022664628",
            "extra": "mean: 2.535313999800002 sec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[0]",
            "value": 1566.0037851727654,
            "unit": "iter/sec",
            "range": "stddev: 0.000041464952185331004",
            "extra": "mean: 638.5680606063654 usec\nrounds: 825"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1]",
            "value": 1016.2888130546235,
            "unit": "iter/sec",
            "range": "stddev: 0.0005375871411835392",
            "extra": "mean: 983.9722598090352 usec\nrounds: 943"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[1000]",
            "value": 6.848631746595733,
            "unit": "iter/sec",
            "range": "stddev: 0.0021736248751169766",
            "extra": "mean: 146.01456714285632 msec\nrounds: 7"
          },
          {
            "name": "bench/bench_flow.py::test_batch_n_events[5000]",
            "value": 1.0913293329447409,
            "unit": "iter/sec",
            "range": "stddev: 0.023613805263251707",
            "extra": "mean: 916.3136826000027 msec\nrounds: 5"
          },
          {
            "name": "bench/bench_flow.py::test_aggregate_df_86420_events",
            "value": 0.05317426030287068,
            "unit": "iter/sec",
            "range": "stddev: 0.04775393943637262",
            "extra": "mean: 18.806091411600015 sec\nrounds: 5"
          }
        ]
      }
    ]
  }
}