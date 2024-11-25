### HDX P-code detector
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/HDX-JP-SW-PCODE/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/HDX-JP-SW-PCODE?branch=main)

This script searches for p-codes in datasets on HDX. It downloads resources and checks the first 200 rows against a list of authoritative p-codes in that location. If more than 90% of them match then it is marked as p-coded in HDX.