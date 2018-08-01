Work on CMOR3 XML database which will replace CMOR2 tables.

Python preparation:
pip install pylibconfig2 pyparsing

Operations order:

bash cpyfromsvn.sh
python convertXML.py
bash createAllTables.sh
bash diffAllTables.sh
bash copyAllTables.sh
