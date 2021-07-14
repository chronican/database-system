# CS422 Project 1 - Report
In this project, I implemented the operators with 3 kinds of constraints in database management system: Run-length-encoding; operator at a time, column at a time.

The Run-length-encoding is the most difficult part because I need to consider lots of situations. 

The operator at a time and column at a time all perform at column store, but they have different operation mechanism.

The part about optimization is simpler than other parts.The function of it is to accelerate the operation.

In each file, I implement the function: open, next, close. I also define other helper functions to implement operator more clearly.

Actually, the part 1, part 4 and part 5 are share the same logical operations, so I implemented them first. The RLE store is need to change each "RLE-entry" (the value,length startVID inside).