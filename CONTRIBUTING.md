# Contributing to Jackdaw

By contributing to Funding Circle, you accept and agree to the following terms
and conditions for your present and future contributions submitted to Funding
Circle. Except for the license granted herein to Funding Circle and recipients
of software distributed by Funding Circle, you reserve all right, title, and
interest in and to your contributions. All contributions are subject to the
following DCO + License terms.

[DCO + License][1]

If you find issues, or have ideas for improvements or new features,
report them to the [issue tracker][2] of the repository. You can also
submit a pull request. Follow these guidelines when you do so.

## Issue reporting

* Make sure that nobody reported the issue before.
* Make sure that the latest code (`master`) does not contain a fix
  already.
* Open an issue with a descriptive title. Give a clear and precise statement of the problem.
* Give the related information about the stack that you use. Include
  the Kafka broker version and the client version.
* Give the version or versions of Jackdaw that you use.
* Add the related code to the issue summary.

Before you report a bug, read the [Troubleshooting section of the
manual][8]. Add the backtrace, sample messages, and topic configurations to
the bug report. This information makes a bug easier to find. Steps that
reproduce the bug also help.

## Signoff on Commits

```
# signoff an individual commit
git commit -s -m 'Add foo feature'

# signoff all commits by default
git config format.signoff true

# git alias to rebase sequence of commits to include signoff
[alias]
  signoff-rebase = "!EDITOR='sed -i -re s/^pick/e/' sh -c 'git rebase -i $1 && while test -f .git/rebase-merge/interactive; do git commit --amend --signoff --no-edit && git rebase --continue; done' -"
```

## Pull requests

* Read [how to contribute to open source projects on Github][3].
* Keep style pull requests and feature pull requests separate. You can discuss proposals in #jackdaw[9]
* Make sure that the unit tests pass (`clojure -M:test`).
* Write [good commit messages][4] and sign each commit (`git commit -s -m 'Add foo feature'`).
* Mention related tickets in the commit messages (e.g. `[Fix #N] Add command ...`).
* Update the [changelog][7].
* [Squash related commits together][6].
* Open a [pull request][5] that relates to *only* one subject. Write a clear title
  and a description in complete sentences.
* [Sign off][10] on all commits. Your signoff certifies that you agree to the [DCO + License][1].

[1]: https://github.com/FundingCircle/jackdaw/tree/master/doc/DCO_+_LICENSE
[2]: https://github.com/FundingCircle/jackdaw/issues
[3]: http://gun.io/blog/how-to-github-fork-branch-and-pull-request
[4]: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
[5]: https://help.github.com/articles/using-pull-requests
[6]: http://gitready.com/advanced/2009/02/10/squashing-commits-with-rebase.html
[7]: https://github.com/FundingCircle/jackdaw/blob/master/CHANGELOG.md
[8]: https://github.com/FundingCircle/jackdaw/blob/master/doc/trouble-shooting.md
[9]: https://clojurians.slack.com/messages/CEA3C7UG0/
[10]: https://www.kernel.org/doc/html/v4.17/process/submitting-patches.html#sign-your-work-the-developer-s-certificate-of-origin
