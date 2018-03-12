
# coding: utf-8

# # CIS 545 Homework 2: Graphs

# In[1]:


# Execute this once, the first time you run
#!pip install networkx

# Disable Python warning messages - you should probably only run this before submission

import warnings
warnings.filterwarnings('ignore')


# ## Step 2.1 Spark Setup

# In[2]:


# TODO: Connect to Spark as per Step 2.1

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
spark = SparkSession.builder.appName('Graphs-HW2').getOrCreate()


# In[ ]:


# Load some dummy data, which should be overwritten in Step 2.2

answers_sdf = spark.createDataFrame([{'from_node': 123, 'to_node': 456},                                    {'from_node': 456, 'to_node': 789},
                                    {'from_node': 456, 'to_node': 890}])
comments_answers_sdf = spark.createDataFrame([{'from_node': 123, 'to_node': 456}])
comments_questions_sdf = spark.createDataFrame([{'from_node': 123, 'to_node': 456}])
graph_sdf = spark.createDataFrame([{'from_node': 123, 'to_node': 456}])


# ## Step 2.2 Loading

# In[ ]:


# TODO: load data as per Step 2.2
answers_sdf = spark.read.load('sx-stackoverflow-a2q.txt', 
                              format="text")
comments_answers_sdf = spark.read.load('sx-stackoverflow-c2a.txt', 
                              format="text")
comments_questions_sdf = spark.read.load('sx-stackoverflow-c2q.txt', 
                              format="text")



# In[ ]:


# You may add as many cells as you like here.
# Use Insert | Insert Cell Below


# ## Step 2.2 Results

# In[ ]:


#answers_sdf.count()


# In[ ]:


##answers_sdf.show(1)


# In[ ]:


#answers_sdf.printSchema()


# In[ ]:


#comments_answers_sdf.count()


# In[ ]:


#comments_answers_sdf.show(1)


# In[ ]:


#comments_answers_sdf.printSchema()


# In[ ]:


#comments_questions_sdf.count()


# In[ ]:


#comments_questions_sdf.show(1)


# In[ ]:


#comments_questions_sdf.printSchema()


# ## Step 2.3

# In[ ]:


# TODO: wrangling work in Step 2.3.  Add as many Cells as you need
# answers_sdf
import pandas as pd
answers_sdf = spark.read.load('sx-stackoverflow-a2q.txt', 
                              format="text")

answers_sdf = answers_sdf.select(F.split(answers_sdf['value'], ' ')[0].alias("from_node").cast("integer"),
                                F.split(answers_sdf['value'], ' ')[1].alias("to_node").cast("integer"))

answers_sdf = answers_sdf.withColumn('edge_type', F.lit('answer'))


# In[ ]:


# comments_questions_sdf
comments_questions_sdf = spark.read.load('sx-stackoverflow-c2q.txt', 
                              format="text")
comments_questions_sdf = comments_questions_sdf.select(F.split(comments_questions_sdf['value'], ' ')[0].alias("from_node").cast("integer"),
                                                   F.split(comments_questions_sdf['value'], ' ')[1].alias("to_node").cast("integer"))

comments_questions_sdf = comments_questions_sdf.withColumn('edge_type', F.lit('comment-on-question'))


# In[ ]:


# comments_answers_sdf
comments_answers_sdf = spark.read.load('sx-stackoverflow-c2a.txt', 
                              format="text")
comments_answers_sdf = comments_answers_sdf.select(F.split(comments_answers_sdf['value'], ' ')[0].alias("from_node").cast("integer"),
                                                   F.split(comments_answers_sdf['value'], ' ')[1].alias("to_node").cast("integer"))

comments_answers_sdf = comments_answers_sdf.withColumn('edge_type', F.lit('comment-on-answer'))


# In[ ]:


# graph_sdf
graph_sdf = answers_sdf.unionAll(comments_questions_sdf).unionAll(comments_answers_sdf)


# ## Step 2.3 Results

# In[ ]:


#answers_sdf.count()


# In[ ]:


#answers_sdf.show(1)


# In[ ]:


#answers_sdf.printSchema()


# In[ ]:


#comments_answers_sdf.count()


# In[ ]:


#comments_answers_sdf.show(1)


# In[ ]:


#comments_answers_sdf.printSchema()


# In[ ]:


#comments_questions_sdf.count()


# In[ ]:


#comments_questions_sdf.show(1)


# In[ ]:


#comments_questions_sdf.printSchema()


# In[ ]:


#graph_sdf.count()


# In[ ]:


#graph_sdf.show(1)


# In[ ]:


#graph_sdf.printSchema()


# ## Step 2.4

# In[ ]:


# You may put any computations you need here
count_answers_sdf = answers_sdf.groupBy("from_node").count()


# ## Step 2.4.1 Results

# In[ ]:


# TODO: output dataframe with top 10 users by number of questions
top_answers_sdf = count_answers_sdf.orderBy(count_answers_sdf['count'].desc())
top_answers_sdf.show(10)


# In[ ]:


# TODO: output top 10 users by number of answers to questions by distinct users
answers_sdf.createOrReplaceTempView('answers_sdf')
distinct_answer_sdf = spark.sql('SELECT from_node, count(DISTINCT to_node) AS a_count FROM answers_sdf  GROUP BY from_node ORDER BY count(DISTINCT to_node) desc')
distinct_answer_sdf.show(10)


# ## Step 2.4.2 Results

# In[34]:


# TODO: number of users whose questions have never been answered but been commented on

#comments_answers_sdf.createOrReplaceTempView('comments_answers_sdf')
#comment_noanswer_sdf = spark.sql('SELECT to_node FROM comments_answers_sdf \
#WHERE to_node NOT IN \
#(SELECT to_node FROM answers_sdf)')

#comment_noanswer_df= comments_answers_sdf.filter(
#    comments_answers_sdf['to_node'].isin(answers_sdf['to_node']) == False)


comments_sdf = comments_questions_sdf.select("to_node")
answer_sdf = answers_sdf.select("to_node")
comment_noanswer_df = comments_sdf.join(answer_sdf, 
                                          comments_sdf['to_node']==answer_sdf['to_node'],
                                          'leftanti')
comment_noanswer_df.distinct().count()



# ## Step 2.4.3 Results

# In[35]:


# TODO: top 10 pairs of users by mutual answers, along with the number of questions they have mutually answered
from pyspark.sql.functions import struct
answers_sdf1 = answers_sdf.filter(answers_sdf.from_node != answers_sdf.to_node)
answers_sdf1=answers_sdf1.withColumn("a_tuple",struct(answers_sdf1.from_node,answers_sdf1.to_node))

count_pair_sdf = answers_sdf1.groupBy("a_tuple").count()
count_pair_sdf = count_pair_sdf.orderBy(count_pair_sdf['count'].desc())


# In[36]:


count_pair_sdf.show(10)


# # Step 3

# In[37]:


# TODO: remove these, which just create dummy data
indegree_sdf = graph_sdf.groupBy("to_node").count()
highest_indegree_sdf = indegree_sdf.orderBy(indegree_sdf['count'].desc())

# TODO: Fill in according to HW spec
outdegree_sdf = graph_sdf.groupBy("from_node").count()
highest_outdegree_sdf = outdegree_sdf.orderBy(outdegree_sdf['count'].desc())


# ## Step 3 Results

# In[38]:


highest_indegree_sdf.show(5)


# In[39]:


highest_outdegree_sdf.show(5)


# ## Step 4

# In[ ]:





# In[ ]:





# In[ ]:





# In[40]:


#
# Step 4.1 Pre-processing
#G: graph_sdf
def spark_bfs(G, origins, max_depth):
    
    schema = StructType([StructField("node", IntegerType(), True)])
    current_sdf = spark.createDataFrame(origins, schema)
    current_sdf = current_sdf.withColumn('depth', F.lit(0))
    current_sdf = current_sdf.repartition(100, 'node').cache()
    visited_sdf = current_sdf[(current_sdf.depth<0)]
    visited_sdf = visited_sdf.withColumn('depth', F.lit(0))
    visited_sdf = visited_sdf.repartition(100, 'node').cache()
    return_sdf = spark.createDataFrame(origins, schema)
    return_sdf = return_sdf.withColumn('depth', F.lit(0))
  
    for depth in range(max_depth):
        
        #new frontier
        frontier_sdf =current_sdf.alias('g1').join(G.alias('g2'),
                                                   F.col('g1.node')==F.col('g2.from_node'),"inner")       
        frontier_sdf=frontier_sdf[['from_node','to_node','depth']]
        # new visited
        
        visited_sdf = visited_sdf.unionAll(current_sdf)
        
        #new current
        current_sdf =frontier_sdf.alias('g1').join(visited_sdf.alias('g2'),
                                            F.col('g1.to_node')==F.col('g2.node'),"leftanti")       
        current_sdf = current_sdf[['to_node']].withColumn('depth', F.lit(depth+1))
        current_sdf = current_sdf.withColumnRenamed("to_node", "node")
        return_sdf = return_sdf.unionAll(current_sdf)
        
    return return_sdf


# ## Step 4.1

# In[41]:


# TODO: comment out this line once your code is ready
bfs_sdf = spark.createDataFrame([{'node': 123, 'depth': 1}, {'node': 456, 'depth': 2}])

# TODO: enable this once your code is ready
origin_map = [{'node': 4550}, {'node': 242}]
bfs_sdf = spark_bfs(comments_questions_sdf, origin_map, 2)
bfs_sdf.count()


# In[ ]:


# TODO: insert code as you like


# ## Step 4.1 Results

# In[42]:


bfs_sdf.show(10)


# ## Step 4.2

# In[43]:


filtered_bfs_sdf = bfs_sdf[(bfs_sdf.depth==2)]
#filtered_bfs_sdf.show(10)


# In[44]:


filtered_bfs_sdf.createOrReplaceTempView('filtered_bfs_sdf')
filtered_bfs_sdf = spark.sql('SELECT node, count(*) AS a_count FROM filtered_bfs_sdf  GROUP BY node HAVING count(*)>1')
#filtered_bfs_sdf.show(10)


# In[45]:


filtered_bfs_sdf.count()


# In[46]:


# Step 4.2 Pre-processing

def friend_rec(input_sdf, graph_sdf):
    input_sdf = input_sdf[['node']]
    input_sdf = input_sdf.repartition(100, 'node').cache()
    input_sdf_c = input_sdf
    input_sdf_c = input_sdf_c.withColumnRenamed("node", "node_c")
    input_sdf_a = input_sdf.crossJoin(input_sdf_c)
    input_sdf_a = input_sdf_a[(input_sdf_a.node != input_sdf_a.node_c)]
    friend_recommendations_sdf = input_sdf_a.join(graph_sdf,(input_sdf_a['node'] == graph_sdf['from_node']) & (input_sdf_a['node_c'] == graph_sdf['to_node']), 'leftanti')
    friend_recommendations_sdf = friend_recommendations_sdf.join(graph_sdf,(friend_recommendations_sdf['node_c'] == graph_sdf['to_node']) & (friend_recommendations_sdf['node'] == graph_sdf['from_node']),'leftanti')
    return friend_recommendations_sdf


# In[47]:


# TODO: insert code as you like


# In[48]:


# TODO: comment this line out when your function works
#friend_recommendations_sdf = spark.createDataFrame([\
#                                                    {'from_node': 123, 'to_node': 456}, \
#                                                    {'from_node': 456, 'to_node': 123}])


# TODO: enable this when your function works

friend_recommendations_sdf = friend_rec(filtered_bfs_sdf, comments_questions_sdf)
friend_recommendations_sdf.count()


# ## Step 4.2 Results

# In[49]:


friend_recommendations_sdf.show()


# ## Step 4.3: Graph visualization

# ### Once you have excecuted the cells in Step 4.2 and you have friend_recommendations_sdf, lets create friend_recommendations_df using toPandas(). This creates an in-memory dataFrame that we can use to build the graph. Here we have used ('from_node','to_node') as column names in friend_recommendations_sdf, please change it to what you have used in yours.

# In[54]:


import networkx as nx

# TODO: create friend_graph NetworkX graph from friend_recommendations_df from friend_recommendations_sdf
friend_recommendations_df = friend_recommendations_sdf.toPandas()  
friend_graph = nx.from_pandas_dataframe(friend_recommendations_df,'node','node_c')
#friend_recommendations_df.head(10)


# ## Step 4.3 Results

# In[55]:


print ("Number of nodes (characters) in this graph is", friend_graph.order()) # number of nodes
print ("Number of edges in this graph is", len(friend_graph.edges())) # number of edges
print ("Graph diameter is", nx.diameter(friend_graph)) # maximum eccentricity


# In[56]:


get_ipython().run_line_magic('matplotlib', 'inline')
nx.draw(friend_graph)

