mkdir tpchdata; mkdir tpchdata/sf0.1
wget https://www.dropbox.com/sh/16xmm2i8v5pmgdr/AADph8McDv8xe7katSkDjRKka?dl=1 -O tpchdata/sf0.1/data.zip
cd tpchdata/sf0.1
unzip -o data.zip
cd ../..
export LEGO_DATA_FOLDER=tpchdata/sf0.1
