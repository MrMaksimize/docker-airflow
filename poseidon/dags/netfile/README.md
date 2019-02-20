In this job, we are hitting the Netfile API to get transactions for different campaign finance reporting forms. There are 460 a, b, c, d, and summary as well as forms 497 and 496. Each form has its own function, because the different forms return slightly different fields that need to be aligned. Some also need processing where we keep rows based on a condition. The first function, get_transactions_a has some extra comments to make clear how all of the functions work in general.

The last two functions, find_new_committees and send_comm_report, are alerts I need to be able to update a companion dataset. These make use of Sonar.