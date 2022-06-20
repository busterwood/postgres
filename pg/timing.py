import pg
c = pg.Connection("postgresql://flyway@api-dev.db.gfknewron.com:5432/newron")

# dt = c.query("""select item_id, outlet_id, period_seq, units::float, price_eur::float, projection_factor::float, is_exclusive
# from sales.item_outlet_sales 
# where country_code = $1 and item_group_code = $2 and periodicity = 'Monthly'""", 'DE', 'MOBILE_COMPUTING')
# for row in dt:
#     print(row)
# exit(0)

c.start_query("""select item_id, outlet_id, period_seq, units::float, price_eur::float, projection_factor::float, is_exclusive
from sales.item_outlet_sales 
where country_code = $1 and item_group_code = $2 and periodicity = 'Monthly'
""", 'DE', 'MOBILE_COMPUTING', binary_format=True)
cur = c.end_query()
while cur.next_row():
    # print(cur.get_int('item_id'), cur.get_int('outlet_id'), cur.get_int('period_seq'), cur.get_float('units'), cur.get_float('price_eur'), cur.get_float('projection_factor'))
    print(cur.get_int(0), cur.get_int(1), cur.get_int(2), cur.get_float(3), cur.get_float(4), cur.get_float(5), cur.get_bool(6))
    # print(cur.get_str(0), cur.get_str(1), cur.get_str(2), cur.get_str(3), cur.get_str(4), cur.get_str(5), cur.get_str(6))
    # print(cur.item_id, cur.outlet_id, cur.period_seq, cur.units, cur.price_eur, cur.projection_factor, cur.is_exclusive)

c.close()