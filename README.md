# Master-Thesis-Information-Management

Here we should upload our code after we are done working on it (in whatever environment you want. I will be using Jupyter Notebooks). So each upload should represent the updated version of the project. 

## Update
* We have already downloaded a bunch of filings using Seppe's code (424B5s and 8-Ks mainly)
* Now we are working on the parsing of the contents of the filings. I have tried two different approaches, and none of them have worked. This is the particular publication I am working with to build the code (https://www.sec.gov/Archives/edgar/data/1174940/000149315220022121/form424b5.htm), after making it work for this particular publication, I have to generalize it to work on any filing. 

### First approach
I noticed the header of the section that contains the risk factors (target header) in the 424B5 filing is in an "a" tag that is embedded within a "p" tag. 
  
![](Screen%20Shot%202020-11-28%20at%2011.09.27.png)
  
Each of the paragraphs under this section are embedded in "p" tags that are siblings to the parent of our target header. Therefore I thought I could extract the contents of the "p" tags that follow the parent tag of the target header that are strings and which have content.
  
### Second approach
Since the first approach did not work, I read on the BS4 documentation that by using an html parser you can print the source-lines and positions of each element in a document. I thought about getting the "coordinates" of each target header, and the header that follows it and scraping every element with coordinates in between those two limits. This approach did not seem to work, I am not sure whether it is because the coordinates are not related to the position of the tags in the document, or whether I did something wrong.
