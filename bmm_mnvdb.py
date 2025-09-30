import os
import sqlite3

class Bmm_MNVDB:

    def __init__(self, databasename) -> None:
        self.databasename = databasename
        if not os.path.exists(self.databasename):
            self.connection = sqlite3.connect(self.databasename)
            c = self.connection.cursor()

            c.execute('''CREATE TABLE IF NOT EXISTS contracts (
                            number TEXT,
                            date TEXT,
                            value INTEGER,
                            partner TEXT,
                            subject TEXT,
                            type TEXT,
                            lemmasubject TEXT,
                            isnew INTEGER)''')

            c.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS contracts_fts 
                            USING FTS5 (number UNINDEXED, partner, subject, lemmasubject, type, tokenize="unicode61 remove_diacritics 2")''')

            c.execute('''CREATE TRIGGER contracts_ai AFTER INSERT ON contracts BEGIN
                            INSERT INTO contracts_fts(number, partner, subject, lemmasubject, type) 
                            VALUES (new.number, new.partner, new.subject, new.lemmasubject, new.type);
                        END;''')

            c.execute('''CREATE TRIGGER contracts_ad AFTER DELETE ON contracts BEGIN
                            INSERT INTO contracts_fts(contracts_fts, number, partner, subject, lemmasubject, type) 
                                VALUES('delete', old.number, old.partner, old.subject, old.lemmasubject, old.type);
                        END;''')

            self.commitConnection()
            c.close()
        else:
            self.connection = sqlite3.connect(self.databasename)

    def closeConnection(self):
        self.connection.close()

    def commitConnection(self):
        self.connection.commit()

    def getContract(self, number):
        c = self.connection.cursor()

        c.execute('SELECT * FROM contracts WHERE number=?', (number,))
        res = c.fetchone()

        c.close()
        return res

    def saveContract(self, number, date, entry, lemmas):
        c = self.connection.cursor()

        c.execute('INSERT INTO contracts (number, date, value, partner, subject, type, lemmasubject, isnew) VALUES (?, ?, ?, ?, ?, ?, ?, 1)',
            (number, date, entry['netTotalValue'], entry['partner'], entry['subject'], entry['type'], lemmas))

        c.close()

    def clearIsNew(self, number):
        c = self.connection.cursor()
        
        c.execute('UPDATE contracts SET isnew=0 WHERE number=?', (number,))

        c.close()

    def searchRecords(self, keyword):
        c = self.connection.cursor()

        c.execute('SELECT * FROM contracts WHERE isnew=1 AND number IN '
                    '(SELECT number FROM contracts_fts WHERE contracts_fts MATCH ?)',
                    (keyword,))

        results = c.fetchall()
        c.close()
        return results
    
    def getAllNew(self):
        c = self.connection.cursor()

        c.execute('SELECT * FROM contracts WHERE isnew=1')

        columns = [description[0] for description in c.description]
        results = [dict(zip(columns, row)) for row in c.fetchall()]
        c.close()
        return results

