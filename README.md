# Dedupe Geocoder

Demonstration app to show how Dedupe might be used as a geocoder

## Setup

**Install OS level dependencies:** 

* Python 3.4
* PostgreSQL 9.4 +

**Install app requirements**

We recommend using [virtualenv](http://virtualenv.readthedocs.org/en/latest/virtualenv.html) and [virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/en/latest/install.html) for working in a virtualized development environment. [Read how to set up virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/).

Once you have virtualenvwrapper set up,

```bash
mkvirtualenv dedupe-geocoder
git clone https://github.com/datamade/dedupe-geocoder.git
cd dedupe-geocoder
pip install -r requirements.txt
cp geocoder/app_config.py.example geocoder/app_config.py
```

In `app_config.py`, put your Postgres user in `DB_USER` and password in `DB_PW`.

Afterwards, whenever you want to work on dedupe-geocoder,

```bash
workon dedupe-geocoder
```

## Setup your database

Before we can run the website, we need to create a database.

```bash
createdb geocoder
```

Then, we run the `loadAddresses.py` script to download our data from the Cook
County data portal.

```bash
python loadAddresses.py --download --load_data 
```

This command will take between 15-45 min depending on your internet connection.

You can run `loadAddresses.py` again to get the latest data from the Cook
County, add more training data, or create a table of block keys for dedupe to
use to match new records. Useful flags are:

```
 --download     Download fresh address data.
 --load_data    Load downloaded address data into database.
 --train        Add more training data and save settings file.
 --block        After training, create the block table used by dedupe for matching.
 ```

## Running Dedupe Geocoder

To run locally:

```
workon dedupe-geocoder
python runserver.py
```

navigate to http://localhost:5000/

## Team

* Eric van Zanten - developer
* Derek Eder - developer
* Forest Gregg - developer
* Cathy Deng - developer

## Errors / Bugs

If something is not behaving intuitively, it is a bug, and should be reported.
Report it here: https://github.com/datamade/dedupe-geocoder/issues

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Commit, do not mess with rakefile, version, or history.
* Send a pull request. Bonus points for topic branches.

## Copyright

Copyright (c) 2015 DataMade. Released under the [MIT License](https://github.com/datamade/dedupe-geocoder/blob/master/LICENSE).
