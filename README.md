# cloudapp-mp2
Machine Programming Assignment for Cloud Application Course

# Build Commands:

## A. TitleCount:
```
hadoop jar TitleCount.jar TitleCount -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt /mp2/titles /mp2/A-output
```

## B. TopTitles:
```
hadoop jar TopTitles.jar TopTitles -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt -D N=5 /mp2/titles /mp2/B-output
```

## C. TopTitleStatistics:
```
hadoop jar TopTitleStatistics.jar TopTitleStatistics -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt -D N=5 /mp2/titles /mp2/C-output
```

## D. Orphan Pages:
```
hadoop jar OrphanPages.jar OrphanPages /mp2/links /mp2/D-output
```
