import time
from dag import Dag

with Dag() as dag: 
    # creates a namespace where all declared functions and variables are added to the DAG
    
    # Define input variables
    a = 3

    # Define functions
    def b():
        time.sleep(2)
        return 2

    def c(a, b):
        return a + b

    def d(a, b, c):
        time.sleep(3)
        return a + b + c
    
    def e(c):
        time.sleep(5)
        return c * 2
    
    def f(d, e):
        time.sleep(1)
        return d + e
    
    def g(a, b):
        time.sleep(1)
        return a - b
    
    def h(a, b, f, g):
        time.sleep(1)
        return "all done"

# Enter the DagRunner
dag.run()

# Get any produced results
result_h = dag.get("h")
print(f"Result h: {result_h}")
