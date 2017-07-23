#! /usr/bin/env python3

import psycopg2


def problem1():
    # What are the most popular three articles of all time?
    db = psycopg2.connect(database="news")
    c = db.cursor()
    query = "select title, count(log.id) as page_hits \
    from articles, log where path similar to '%' || slug \
    and status not similar to '%404 NOT FOUND%' \
    group by title order by page_hits desc limit 3;"
    c.execute(query)
    lines = c.fetchall()
    print("\nThe most popular three articles of all time are:\n")

    for i in lines:
        print(' "%s" - %s views ' % (i[0], i[1]))
    db.close


def problem2():
    # Who are the most popular article authors of all time?
    db = psycopg2.connect(database="news")
    c = db.cursor()
    query = "select name, count(*) as page_hits \
    from articles, authors, log where articles.author = authors.id \
    and path similar to '%' || slug \
    and status not similar to '%404 NOT FOUND%' \
    group by name order by page_hits desc;"
    c.execute(query)
    lines = c.fetchall()
    print("\nThe most popular authors of all time are:\n")

    for i in lines:
        print(' %s - %s views ' % (i[0], i[1]))
    db.close


def problem3():
    # On which days did more than 1% of requests lead to errors?
    db = psycopg2.connect(database="news")
    c = db.cursor()
    query = "with page_hits as (select time::date\
    as time_stamp, sum(case when status similar to \
    '%200 OK' then 1 else 0 end)\
    as good_views, sum(case when status similar to \
    '%404 NOT FOUND'  then 1 else 0 end)\
    as bad_views, count(*) as total from log group by time::date \
    order by time_stamp)\
    select time_stamp, (bad_views/total::float)\
    as total_errors from page_hits where bad_views/total::float > .01;"
    c.execute(query)
    lines = c.fetchall()
    print("\nOn the following days more than 1% of requests led to errors:\n")

    for i in lines:
        print(' %s - %8.2f%% Errors ' % (i[0], i[1] * 100))
    db.close


if __name__ == '__main__':
    problem1()
    problem2()
    problem3()
