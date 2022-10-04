# Reducing the Impact of Behavioral Health Crises 

### *[Data Science for Social Good](https://www.dssgfellowship.org/) 2022 Summer Fellowship, Carnegie Mellon University* 

## Fellows
  ### [Núria Adell Raventós](https://github.com/nuriaar)
  ### [Fabian Dablander](https://fabiandablander.com/)
  ### [Juan Luque](https://github.com/jdluque)
  ### [Victoria Ritvo](https://github.com/vej)

## Mentors
  Erika Salomon, Liliana Millán Núñez

## Partners
-  Mental Health Center in Johnson County, Kansas
- Criminal Justice Coordinating Council in Douglas County, Kansas


## Goal

Behavioral health crises are a pernicious issue in the US. In 2020, there were over 45K deaths by suicide, and over 1.2 million suicide attempts. In 2021, there were over 107K deaths due to drug overdoses (Center for Disease Control and Prevention, 2022). 


We partnered with the Johnson and Douglas Counties in Kansas to tackle this problem in their local contexts. Currently, our partners have outreach teams that work based on referrals to offer care to those in need. Our goal is to provide a proactive solution and use machine learning to predict people at risk of a behavioral health crisis to recommend for preventative outreach.


## Requirements and Installation
- Linux/Bash Terminal (to run the scripts)
- Python 3.10.4
- PostgreSQL 22.1.1

To create the environment with all the necessary tools installed (including underlying C libraries and python requirements):
```
conda hivenv create -f environment.yml
conda activate dojo_mh
```
Our environment here is called 'dojo_mh'

To allow programmatic access to the database, create an environment variable as:
```
export DBURL="postgres://your_username:your_password@url_to_database:xxxx/database_name"
```
where xxxx is the port number. You can also add this to your bash profile so it is available by default when you
access the terminal. To add it to your bash profile:
```
echo "export DBURL="postgres://your_username:your_password@url_to_database:xxxx/database_name" > ~/.bashrc
sh ~/.bashrc
```
The database can then be accessed using `psql`or `SQLAlchemy` using the connection string.

## ETL
The files and workflow to extract the raw data, clean them, and upload them to a database are described in:

- [ETL explanation](https://github.com/dssg/dojo_mh/tree/dev/infrastructure/ETL)

## Pipeline
The files and workflow to run the pipeline are described in:

- [Pipeline explanation](https://github.com/dssg/dojo_mh/tree/dev/src/pipeline)

## Project Poster
<figure>
  <img src="https://github.com/dssg/dojo_mh/blob/main/content/dojo_poster.png" alt="Poster for machine learning project"/>
</figure>

