zargan-network
==============

 * Put the input files in data folder: stats20110912-01.txt
 * Run the clear script. This will clear the corrupted lines:

  python zargan/clear.py
 
 * Now run the actual command:

  python zargan/process.py zargan/data/filtered.txt 1000 300 3
  
 * filtered.txt is the input file
 * 1000 is the number of lines read from the input file.
 * 300 seconds is the window size.
 * 3 is the pruning threshold. Edges with weight<3 will be removed.

  
  

