import os
for root, dirs, files in os.walk():
   for name in files:
      print('\t%s' % name)