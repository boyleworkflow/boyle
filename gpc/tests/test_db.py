from gpc.fsdb import Database
import uuid

def main():
    #Database.create('abc', open('../../database.sql', 'r').read())
    db = Database.load('abc')
    uid = '1234'
    name = 'rasmus2'
    #db.execute('INSERT INTO usr(id, name) values (?, ?)', (uid, name))

    for row in db.execute('SELECT * from usr'):
        print(row)

if __name__ == '__main__':
    main()