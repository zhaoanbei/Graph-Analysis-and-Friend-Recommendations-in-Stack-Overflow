
# coding: utf-8

# In[117]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
spark = SparkSession.builder.appName('Graphs-HW2').getOrCreate()


# In[118]:


# Read lines from the text file
pr_sdf = spark.read.load('pr_graph.txt', format="text")
pr_sdf = pr_sdf.select(F.split(pr_sdf['value'], ' ')[0].alias("from_node").cast("integer"),
                                F.split(pr_sdf['value'], ' ')[1].alias("to_node").cast("integer"))
print('how many link',pr_sdf.count())
pr_sdf.show()


# In[119]:


count_sdf = pr_sdf.groupBy('from_node').count()
num_node = count_sdf.count()


# In[120]:


rank_sdf=count.withColumn('rank',F.lit(1/num_node))
rank_sdf=pr_sdf1[['from_node','rank']]
rank_sdf.show()


# In[121]:


rank_sdf.createOrReplaceTempView('rank') #pr_sdf1


# In[122]:


pr_sdf.createOrReplaceTempView('nodes') #from_node|to_node 16
#link counts
pr_sdf2 = spark.sql('SELECT from_node, COUNT(to_node) AS count FROM nodes GROUP BY from_node ') # from_node|count

#weight
pr_sdf2.createOrReplaceTempView('result1')
weights_sdf = spark.sql('SELECT r1.from_node,r1.to_node, (1/r2.count) AS weight FROM result r1 LEFT JOIN result1 r2 WHERE r1.from_node = r2.from_node')
weights_sdf.createOrReplaceTempView('weights')

weights_sdf.show()


# In[124]:


sin_count_sdf = spark.sql('SELECT r2.*, ((r.rank*r2.weight)*0.85) AS rank FROM rank AS r LEFT JOIN weights AS r2 WHERE r.from_node = r2.from_node ')
print(sin_count_sdf.show(10))


# In[126]:


sin_count.createOrReplaceTempView('sin_count')
pr_values_sdf = spark.sql('SELECT to_node AS from_node,(SUM(rank)+0.15) AS rank FROM sin_count GROUP BY to_node')
pr_values_sdf.show()


# In[ ]:





# In[127]:


def pagerank(G, num_iter):
    
    G.createOrReplaceTempView('g_sdf')
    count_sdf = pr_sdf.groupBy('from_node').count()
    num_node = count_sdf.count()
    rank_sdf=count.withColumn('rank',F.lit(1/num_node))
    rank_sdf=pr_sdf1[['from_node','rank']]
    rank_sdf.createOrReplaceTempView('rank')
    pr_sdf.createOrReplaceTempView('nodes') #from_node|to_node 16
    #link counts
    pr_sdf2 = spark.sql('SELECT from_node, COUNT(to_node) AS count     FROM nodes     GROUP BY from_node ') # from_node|count

    #weight
    pr_sdf2.createOrReplaceTempView('result1')
    weights_sdf = spark.sql('SELECT r1.from_node,r1.to_node, (1/r2.count) AS weight     FROM result r1     LEFT JOIN result1 r2     WHERE r1.from_node = r2.from_node')
    weights_sdf.createOrReplaceTempView('weights')


    for i in range(0,num_iter):
        sin_count_sdf = spark.sql('SELECT r2.*, ((r.rank*r2.weight)*0.85) AS rank         FROM rank AS r         LEFT JOIN weights AS r2         WHERE r.from_node = r2.from_node ')
        
        sin_count.createOrReplaceTempView('sin_count')
        pr_values_sdf = spark.sql('SELECT to_node AS from_node,(SUM(rank)+0.15) AS rank         FROM sin_count         GROUP BY to_node')
                
        pr_values_sdf.createOrReplaceTempView('rank')


    return pr_values_sdf


# ## Step 5

# In[128]:


pr_sdf.count()


# In[129]:


pr_sdf.show()


# In[130]:


pagerank(pr_sdf, 5).orderBy("to_node").show()

