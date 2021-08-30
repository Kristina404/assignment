# Assignment
A program compares the product data between the `before CSV` and
the `after CSV` through implementation of the logic described below:
## Logic
    - Create: the product wasn’t imported from the eCommerce system yesterday,
        but it was imported today. This means we have to send a create operation
        to the eCommerce platform.
    - Update: the product was imported yesterday and is also imported today,
        however, one of the values for the products has changed (e.g. the price of
        the product). This means we have to send an update operation to the
        marketing channel.
    - Delete: the product was imported yesterday, but was not imported today.
        This means we have to send a delete operation to the marketing channel.
  ## Technologies
    - Python 3.8
  ## Installation 
``pip install -r requirements.txt``