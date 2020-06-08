# mapping_repo

To run, navigate to the folder with mapping.py and type the following into your terminal.

```console
user@mac:~$ python3 mapping.py
```

High-Level Logic:
1. The program will first read the CSV files into their respective data frames. 
2. Then, data frames will be cleaned. Cleaning includes removing special chars from strings. Changing data types so they are consistent between datasets. The casing will be modified.
3. Next, datasets will be enriched with the Thesaurus to standardize the fuel types. Dominate plant names will be isolated.
4. Headers will also be renamed to reflect the final output.
5. Once the cleaning and processing of the data is complete, the 3 datasets will be merged on the dominant plant name, fuel type, and country.
6. Mismatches from Platts will be put through a tiebreaker process. This process will attempt to remove the mismatches by looking at the unit name suffix (such as "3" in unit name "Ingolstadt 3", and similar energy production by mw.  From my data exploration, extra matches with GPPD are usually the same plant since GPPD only includes plant names and not unit names. Thus,
   we can drop all but one of the rows that have all 3 identifiers.
7. Lastly, the cleaned up and merged data frame will be saved as a CSV file.

