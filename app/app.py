from flask import Flask, render_template, request
from flask import jsonify
from cassandra.cluster import Cluster
import json

app = Flask(__name__)

# 127.0.0.1:9042

cluster = Cluster(["127.0.0.1"], port="9042")
session =   cluster.connect('stackoverflow')

@app.route('/')
def user():
   return render_template('home.html')



@app.route('/dashboard',methods = ['POST', 'GET'])
def result():
   
   if request.method == 'POST':
      userid = request.form['userid']
      stmt1 = " Select * from userprofile where id ='"+ str(userid) + "';"
      stmt2 = "Select tag from toptags where userid = '"+ str(userid)+ "'  LIMIT 5;"
      stmt3 = " Select questions  from usertoquestion where userid= '"+ str(userid)+ "'  LIMIT 5;"  
      stmt4 = " Select tagname,count from trendingtags LIMIT  10 ;"  
      stmt5 = " Select reputation from realtimereputations where id ='"+ str(userid) + "';"
      response1 = session.execute(stmt1)
      response2 = session.execute(stmt2)
      response3 = session.execute(stmt3)
      response4 = session.execute(stmt4)
      response5 = session.execute(stmt5)
      
          
      
          
          
      return render_template("dashboard.html" , response1= response1, response2= response2 , response3 = response3 , response4 = response4 , response5 = response5 )
      

      
if __name__ == "__main__":
    app.run(host='localhost', port=5001, debug=True)
    
    
    


