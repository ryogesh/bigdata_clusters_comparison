# Compare properties differences between Big Data Clusters
## Overview

Compares the xml property files e.g core-site.xml, hdfs-site.xml etc. between 2 clusters. Comes handy when comparing big data cluster differences. Few xml properties are ignored during comparison because they will be different across clusters. Look for the variable "xml_props_ign" in the script and add/delete the ignore properties.

Store all xml property files for a base cluster in one folder, it's referred as left or template cluster. Store all xml property files of the comparing cluster in another folder, it's referred to as right cluster. Run the script. The script will analyze the differences of each xml file.If property differences are found, or if there are missing properties, then a corresponding .txt file will be created in the results folder.
e.g. After comparing hdfs-site.xml between the cluster, if there are differences or missing properties then "resultfolder"/hdfs-site.txt is created.


### Running the script
   Default left folder is /tmp/confL , right folder is /tmp/confR and results folder is $HOME/confL_confR

   ```
   [user@localhost ~]$ python compare_clusters.py
   ```    
   
   Specifying folders

   ```
   [user@localhost ~]$ python compare_clusters.py --left /tmp/L --right /tmp/R --resultdir /tmp/clusterB_cluster1
   ```    
   
    Help on the script
  
   ```
   [user@localhost ~]$ python compare_clusters.py -h
  usage: compare_clusters.py [-h] [--left LEFT] [--right RIGHT]
							   [--resultdir RESULTDIR] [--missingprops {y,n}]

	Compare property differences between 2 clusters

	optional arguments:
	  -h, --help            show this help message and exit
	  --left LEFT           Left cluster folder with xml property files, Default: /tmp/confL
	  --right RIGHT         Left cluster folder with xml property files, Default: /tmp/confR
	  --resultdir RESULTDIR Folder to save the comparison files, Default: /home/user
	  --missingprops {y,n}  Print missing properties between clusters, Default: y
   ```    
   
