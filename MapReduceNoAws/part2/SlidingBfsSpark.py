from pyspark import SparkContext
import Sliding, argparse

def bfs_map(pair):
    """ YOUR CODE HERE """
    if pair[1] == level:
        for child in Sliding.children(WIDTH, HEIGHT, Sliding.hash_to_board(WIDTH, HEIGHT, pair[0])):
            yield (Sliding.board_to_hash(WIDTH, HEIGHT, child), pair[1] + 1)
    yield pair

# We used this before realizing the built-in min can just be passed into our reduce
# Kept for reference

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    return value1 if value1 < value2 else value2

# Filter used at the end of some while loops, takes in the level that has just been added to dataset
# Returns a function that allows filtering out data that isn't in the current frontier level

def end_filter(pair):
    if pair[1] == level:
        return True
    return False

def solve_puzzle(master, output, height, width, slaves):
    global HEIGHT, WIDTH, level
    HEIGHT=height
    WIDTH=width
    level = 0

    sc = SparkContext(master, "python")

    """ YOUR CODE HERE """
    numBuckets = 16
    actionStep = 4
    partitionStep = 12
    # tracks the nth iteration loop is on. Effectively starts at one due to incremement in beginning
    iters = 0

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    currDD = sc.parallelize([(Sliding.board_to_hash(WIDTH, HEIGHT,
        (Sliding.solution(WIDTH, HEIGHT))), 0)])

    while True:
        iters += 1
        currDD = currDD.flatMap(bfs_map).reduceByKey(min)
        level += 1

        # every actionStep iterations, including the first one, remove backwards moves (duplicate boards)
        # calculate the number of elements in this iteration's frontier
        if iters % actionStep == 1 and currDD.filter(end_filter).count() == 0:
            break

        # every partitionStep iterations starting from the partitionStep-th iterations, shuffle the data
        # with the sufficient default hash for optimized reducing
        if iters % partitionStep == 0:
            currDD = currDD.partitionBy(PARTITION_COUNT)

        # If nothing in the frontier, signifies all boards have been found and we are done

    # Switch the level with board so data is sorted by level; sort levels
    currDD = currDD.map(lambda pair: (pair[1], pair[0])).sortByKey()

    currDD.coalesce(slaves).saveAsTextFile(output)
    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
