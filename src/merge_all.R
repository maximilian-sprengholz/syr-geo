#
# This file:
# - merges all context data from different sources for the same geo level
# - in case the ars entities varies over time, we apply a POP or AREA weighted merging (crosswalk?)
#

# As we have to do the crosswalks IN ANY CASE, it should be ok to do all the merging for different
# entities: will just be time consuming

# strip all parentheses from varnames (other Stata rules?)

