# Code Review Guide

## Principles of code review

The goal of code review is for the author and reviewer(s) to collaboratively produce the best, highest-quality PR they can.

* Review the code based on its own merits, not based on how you would have personally written it.

* Articulate clear and justifiable reasons for your decisions and feedback.

* Take the perspective of someone who has to read and understand the code in the future.

* Evidence and technical reasoning override preference and personal opinion.

* Feedback should always be about the *code*, not the *person*.


## Before you start reviewing

* Read the description (problem summary, what changed and how does it work) to understand the goal of the PR. If it's not easy to understand, ask the author to improve it.

  * In particular, AI-generated PR descriptions can be extremely long and unnecessarily verbose. In these cases, gently ask the author to read the description themselves and try to make it more concise.

* Make sure you are familiar with the packages the PR modifies. If you are not, read the surrounding code to understand the context of the change, or ask the author clarifying questions.

* Make sure you have enough continuous time to review the PR, use 300 LOC per hour to estimate.

* Make sure you can follow the updates of the PR in the next few work days.

* **You must read and understand every change in the PR.** If this is not feasible, the PR is too big. Ask the author to split the PR into smaller PRs that are easier to review thoroughly.

  * A good rule of thumb is to scope each PR as one minimal, atomic change. Large features can then be implemented as a "stack" of PRs that build on top of each other.

  * Try to separate cleanup and refactoring changes into a separate PR so they don't clutter up PRs that are making semantic changes with unnecessary noise.

  * Some types of changes, especially from cleanups or refactors, are more mechanical (for example, renaming every usage of a field). Every instance should still be reviewed, but they don't need the same scrutiny as more semantic code changes.

* For a bug fix PR, if there is no test case, ask the author to add tests.

* For a performance PR, if no benchmark result is provided, ask the author to add a benchmark result.


## Recommended review process

1. **Skim the changes to get a high-level understanding of what is going on.** Does it seem to follow from the intent conveyed by the PR description?

  * Does the overall approach seem reasonable? Do the changes roughly make sense? If not, this is an issue that needs to be raised.

2. **Closely read each change carefully, as if you were modifying or refactoring the code yourself.**

  * If existing code is being modified or removed, ensure you understand what it did and why it is being removed. If that is not clear, raise an issue.

  * For new code, can you follow what each line is doing? Does it follow from what the PR described? If not, this is an issue that needs to be addressed.

3. **Critical thinking and suggestions.** Are there specific, tangible ways to improve the code being changed?

  * **Necessity:** Does the code directly address the problem, or are there unnecessary complications? Does it duplicate or reimplement logic or services provided by other parts of the codebase? (If so, the code is unnecessarily redundant, and should leverage or improve the functionality that already exists.)

  * **Sufficiency:** Does the code fully address what the PR says it does? Are there cases or usages that were missed or not considered?

  * **Robustness:** Try to imagine ways the new code could break or fail, including performance and scalability bottlenecks. Are failure modes reasonably addressed?

  * **Usability:** Try to imagine how the code will be used, either by end-users or other parts of the codebase. Would things make sense to someone unfamiliar with the specific implementation details?

  * **Style:** Are there simpler or more direct ways to implement the PR? Style feedback should generally just be a suggestion rather than blocking, unless the implementation is especially confusing or unnecessarily complicated.


## Things to check during the review process

* Is the code written following the style guide?

* Is there duplicated code?

* Do comments exist and describe the intent of the code?

  * Consider the likelihood of a comment going stale, especially if it refers to something else that must be kept in sync. A stale and misleading comment can be more harmful than useful to future readers of the code.

  * Ask yourself, "If this comment didn't exist, would it make the code more difficult to understand?"

* Are hacks, workarounds and temporary fixes absolutely necessary, or can they be replaced by more robust and general solutions?

  * If they are, are they at least commented?

* Can a function's behavior be inferred by its name, or does the function do more than its name suggests?

* Do tests exist and are they comprehensive?

  * If the PR is fixing a bug, ensure that test cases actually address the issue. Would the test still pass if the bug wasn't fixed? If so, it's not a valid test.

* Am I able to understand the purpose of each unit test?

* Do unit tests actually test that the code is performing the intended functionality?

* Do unit tests cover all the important code blocks and specially handled errors?

* Could procedural tests be rewritten to table-driven tests?

  * A common pattern is to define an array of structs that define the input and expected output, and then loop over every struct in the array. The [DDL AST tests](pkg/parser/ast/ddl_test.go) contain many examples of this pattern.

  * Many larger tests use "test suites": JSON files of cases and expected output that decouple the test code from the test data. For example, the [planner indexmerge tests](pkg/planner/core/casetest/indexmerge/main_test.go) load JSON files from a [testdata subdirectory](pkg/planner/core/casetest/indexmerge/testdata).


## Guidance for reviewers

* Take care to ensure that your feedback is about the *code*, not the *author*: feedback should never be personal. Be kind to the coder, not to the code.

  * A good rule of thumb is to use "we" rather than "you": for example, "We shouldn't be calling this repeatedly" is better than "You shouldn't be calling this repeatedly". This both avoids the risk of interpreting feedback as personal attacks, frames code review as a collaborative effort between the author and the reviewer, and emphasizes the shared ownership of the codebase.

* Ask questions rather than make statements.

* Treat people who know less than you with respect, deference, and patience.

* Remember to praise when the code quality exceeds your expectation.

* It isn't necessarily wrong if the coder's solution is different than yours.

* Be mindful of how much work you are asking the author to do and whether it is within scope of the PR. Pre-existing problems in the area being modified are not necessarily the author's responsibility to fix in the current PR.

  * As a compromise, consider asking the author to create a follow-up issue or PR.

* Consider separating feedback into three tiers: blocking issues, suggestions, and nit picks. These decrease in importance:

  * **All blocking issues must be addressed or have a resolution/compromise worked out with the reviewer.**

  * Suggestions and nit picks are non-blocking forms of feedback. The author gets to decide if they want to incorporate them or not. Either way, the reviewer should not block approval based on these.

    * A suggestion might be, for example, a name for a variable or field that is acceptable, but could be clearer or more idiomatic.
  
    * A nit pick might be, for example, fixing a typo or awkwardly-phrased comment, removing extraneous parentheses around a Boolean condition, or simplifying nested if conditionals with a Boolean `&&` operator.

    * That said, be careful to not use their personal preferences as a reason to excessively nit pick.


## Guidance for authors

* Respond to all feedback raised by the reviewer, and address all blocking issues.

  * In general, prefer to incorporate non-blocking suggestions from the reviewer. If you disagree and choose not to, clearly articulate your reasoning for why.

* If the reviewer points out an issue in one part of the PR, proactively look for and address similar issues in the rest of the PR before re-requesting a review. The reviewer should not have to raise the same kind of issue repeatedly.

* When in doubt, generally lean towards doing what the reviewer asks. The reviewer represents future readers of the code.

* If the PR is time-sensitive, make this expectation clear, and the reviewer should take this into account ("is this really important enough to block my approval of the PR?").

  * However, never rush or pressure the reviewer to approve a PR due to time constraints.


## Things to remember after you have submitted a review

* **Never approve a PR if there are outstanding blocking issues that the author has not addressed.**

* Check GitHub notifications or emails regularly to keep track of updates to the PR.

* When the PR has been updated, start another round of review or give it a LGTM.
