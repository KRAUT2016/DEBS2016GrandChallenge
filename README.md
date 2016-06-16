# GraphCEP for the DEBS 2016 GrandChallenge

This project for complex event processing on graph structured data solves the DEBS 2016 Grand Challenge and ranks among the top performant solutions.

More details about the DEBS 2016 Grand challenge, you can find [here](http://www.ics.uci.edu/~debs2016/call-grand-challenge.html "DEBS 2016 Grand Challenge"). For a more detailed description of our system, have a look at our paper: [GraphCEP](http://www2.informatik.uni-stuttgart.de/cgi-bin/NCSTRL/NCSTRL_view.pl?id=INPROC-2016-17&mod=&engl=&inst=VS "Grand  Challenge: GraphCEP - Real-time Data Analytics Using Parallel Complex Event and Graph Processing").

## Reference

Bibtex:
@article{mayer2016grand,
  title={Grand Challenge: GraphCEP-Real-time Data Analytics Using Parallel Complex Event and Graph Processing},
  author={Mayer, Ruben and Mayer, Christian and Tariq, Muhammad Adnan and Rothermel, Kurt},
  year={2016}
}

## Quickstart
1. Download the source code
2. Download input data files from [here](http://www.ics.uci.edu/~debs2016/call-grand-challenge.html "DEBS 2016 Grand Challenge")
3. Store input data in a folder next to your source files (data/)
4. Run class ManagementMain: java ManagementMain data/friendships.dat data/posts.dat data/comments.dat data/likes.dat 3 43200
5. The last two parameters are k for the top-k comments with the largest communities, and d for the duration in seconds each comment is valid. "Given an integer k and a duration d (in seconds), find the k comments with the largest range, where the range of a comment is defined as the size of the largest connected component in the graph defined by persons who (a) have liked that comment (see likes, comments), (b) where the comment was created not more than d seconds ago, and (c) know each other (see friendships)." - [here](http://www.ics.uci.edu/~debs2016/call-grand-challenge.html "DEBS 2016 Grand Challenge")