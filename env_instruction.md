conda create --name pmc_oa python=3.9 -y
conda activate pmc_oa
poetry install









# notes about pmc ftp download:

The bulk download service only contains xml files. To get the media files we have to use the PMC FTP service - more specifically, individual article download.

If you only want to download some of the PMC OA Subset based on search criteria or if you want to download complete packages for articles that include XML, PDF, media, and supplementary materials, you will need to use the individual article download packages. To keep directories from getting too large, the packages have been randomly distributed into a two-level-deep directory structure. You can use the file lists in CSV or txt format to search for the location of specific files or you can use the OA Web Service API

The oa_file_list.csv contains a column with the randomized path to the tar.gz file for a particular article, e.g. for PMC555938 you can get: oa_package/66/8b/PMC555938.tar.gz. This randomization is the reason why I think we can’t simply wget or curl to download the files after constructing a predictable path.

Since there are so many articles, I planned to utilize their license and volume structure. For the open access subset there are three license types: commercial, noncommercial, and other. Each of these license types has 10 to 12 volumes. Each volume contains anywhere between 1k-500k article tar.gz files. Each of these volumes have their own file list csv that lists all the articles that fall under that volume

I wrote my code such that the user can download all these file lists and allow for downloading each tar.gz file based on the volume and license.

