<p align="left">
<img src="./img/moosez.png" width="1000px" height="450px" >
</p>

I extensively use the [giraffez](https://github.com/capitalone/giraffez) library in my daily work with Teradata. Outside of work, I mostly use PostgreSQL databases. I am consistently copying and pasting a database operations module (a set of psycopg2 wrapper functions) across projects, making various tweaks as I go (and directly violating [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)...). As a result, I decided to create a package with similar functionality to giraffez, where the database connection is opened and automatically closed through Python's [with](http://effbot.org/zone/python-with-statement.htm) construct; a much better way for managing resources.

As for a name? The project is largely based of the giraffez package, and, I am Canadian, so *moosez*.

**WORK IN PROGRESS***

## Installation

## Usage

### Resources
* [Docstring convention](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
* [giraffez](https://github.com/capitalone/giraffez)
*
