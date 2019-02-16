---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.0'
      jupytext_version: 0.8.6
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# New Driver Splits

A complete rewriting of driver splits datagrabbing:
    
- make more effective use of SQL queries;
- ensire that pivoting and rebasing are done using unique `entryId` values rather than possibly duplicate `drivercode` values;
- test each step with multiplce classes, eg `RC1`, `RC2`, *Junior WRC*.

```python

```
