# Code Review Guide

## Things to do before you start reviewing the PR

* Make sure you are familiar with the packages the PR modifies.

* Make sure you have enough continuous time to review the PR, use 300 LOC per hour to estimate.

* Make sure you can follow the updates of the PR in the next few work days.

* Read the description of the PR, if it's not easy to understand, ask the coder to improve it.

* For a bug fix PR, if there is no test case, ask the coder to add tests.

* For a performance PR, if no benchmark result is provided, ask the coder to add a benchmark result.


## Things to check during the review process

* Am I able to understand the purpose of each unit test?

* Do unit tests actually test that the code is performing the intended functionality?

* Do unit tests cover all the important code blocks and specially handled errors?

* Could procedure tests be rewritten to table driven tests?

* Is the code written following the style guide?

* Is the same code duplicated more than twice?

* Do comments exist and describe the intent of the code?

* Are hacks, workarounds and temporary fixes commented?

* Does this function do more than the name suggests?

* Can this function's behavior be inferred by its name?

* Do tests exist and are they comprehensive?

* Do unit tests cover all the important code branches?

* Could the test code be extracted into a table-driven test?


## Things to keep in mind when you are writing a review comment

* Be kind to the coder, not to the code.

* Ask questions rather than make statements.

* Treat people who know less than you with respect, deference, and patience.

* Remember to praise when the code quality exceeds your expectation.

* It isn't necessarily wrong if the coder's solution is different than yours.

* Refer to the code style document when necessary.


## Things to remember after you submitted the review comment

* Checkout Github notification regularly to keep track of the updates of the PR.

* When the PR has been updated, start another round of review or give it a LGTM.
