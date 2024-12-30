# Trading Algorithm
## Outline
### Resistance Line
1. From left to write
2. Find the local maxima, in the interval each value cannot greater than the resistance line
3. Find the most point

Note: the slope may less than or equal to 0

Implementation
```txt
Two pointer method
1. A pointer (A) is in the beginning
2. the other pointer (B) is increment one index
if val(A) >= val(B) then
    Add the B index to the next resistance point
    continue
else
    record this point and continue, because this may be false breakout
    
```

### Support Line

Note: the slope may less than or equal to 0