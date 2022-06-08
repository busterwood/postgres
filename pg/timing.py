import libpg as pg
c = pg.Connection("postgresql://flyway@api-dev.db.gfknewron.com:5432/newron")
c.start_query("""select item_id, outlet_id, period_seq, units::float, price_eur::float, projection_factor::float
from sales.item_outlet_sales 
where country_code = $1 and item_group_code = $2 and periodicity = 'Monthly'""", 'DE', 'MOBILE_COMPUTING')
cur = c.end_query()
while cur.next_row():
#   print(cur.get_int('item_id'), cur.get_int('outlet_id'), cur.get_int('period_seq'), cur.get_float('units'), cur.get_float('price_eur'), cur.get_float('projection_factor'))
#   print(cur.get_int(0), cur.get_int(1), cur.get_int(2), cur.get_float(3), cur.get_float(4), cur.get_float(5))
   print(cur.get_value(0), cur.get_value(1), cur.get_value(2), cur.get_value(3), cur.get_value(4), cur.get_value(5))
