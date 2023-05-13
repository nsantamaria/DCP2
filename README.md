# Changes in Online Political Advertisements
#### IDC 5131: Distributed Computing

Aaron Spielman, Paul Hwang, Fehmi Neffati, Nikolas Santamaria, Joshua Ingram

## Project Overview

Baab is a journalist who covers politics for the primary newspaper in a nearby urban area. Baab has reason to believe that online political advertising is changing and he thinks there may be a big story here. He is trying to convince his management to fund a larger investigation into online advertising that he believes could lead to a series of in-depth stories that would garner national attention for his paper. However, he needs ammunition to convince his management that there is actually an interesting story here.

Baab is hoping that you will find something interesting to help him convince his management. However, if you can convince him that no interesting changes are taking place, you can prevent Baab from going off on a wild goose chase that will cost his company a ton of money and damage his career.

## Project Contributions

*Joshua Ingram*
- Project management
- Analysis of advertisement frequency (R)
- Presentation slides and organization

*Aaron Spielman*
- Sentiment analysis of advertisements (Python)
- Analysis of top advertisers (Python, R)
- Presentation Slides

*Paul Hwang*
- Analysis of advertisement targets (R)
- Text analysis of advertisements (Python, R)
- Presentation Slides

*Fehmi Neffati*
- Collection and formatting of advertisement target data (Python)
- Data Munging (Python)

*Nikolas Santamaria*
- Research on advertisement data collection process
- Presentation Slides

## Project Summary

After collecting and munging the advertisement data from Propublica and AdObserver, we took a number of analysis approaches to analyzing any trends in the advertisement data over time. Throughout each analysis, we did not find any major temporal trends in the data. Any possibly interesting artifacts in the data ended up being the result of poor and biased data through the collection methodologies. We further investigated the data collection process and quality of data provided for our analysis, finding the data is not reliable to make the kinds of inferences requested by the client. It is our recommendation to the client to seek out more and better data, to use other methods to identify trends in the online political advertisements, or to pursue other projects.

## About this Repository

This repository is broken up into folders based on focus.

*data_analysis_visualization* - Files focused on analyzing and visualing data. Some data munging.

*data* - Data files. (Note: main data files are too large and not on Github)

*data_munging_eda* - Data munging and exploratory data analysis files.

*documentation* - PDFs, slides, logbook, etc.
