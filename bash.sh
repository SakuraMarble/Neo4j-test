#!/bin/bash
python write_name.py
for i in {1..20}
do
   python main.py
done
python average.py