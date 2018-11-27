import sqlite3
import pickle
conn = sqlite3.connect('WIKI.sqlite')
cursor = conn.cursor()
name ="John"
list =[10091,10530,12165,12937,12,922,1093,1134,5985,6203,8263,9059,9730,10674,12656,808,1043,1166,1593,1702,1730]
data=pickle.dumps(list)
cursor.execute("INSERT INTO users(name,articles) VALUES(?,?)", (name,sqlite3.Binary(data)))
conn.commit()
cursor.execute("SELECT articles FROM users")
