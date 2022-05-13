# objective: maximize payoff area as sum of all expected payoffs
# constraints: no more than 4 put options in the same strike
# constraints: no more than 8 put options in the strategy
# constraints: no more than 4 call options in the same strike
# constraints: no more than 8 call options in the strategy
# constraints: at least +/- x from current strike
# optional constraints: margin <= 3000 - to be calculated from broker platform

# Short PUT: MIN(0,(strike-underlying price at expiration) * # contracts * contract size)
# Long PUT: MAX(0,(strike-underlying price at expiration) * # contracts * contract size)
# Short CALL: MIN(0,(underlying price at expiration-strike) * # contracts * contract size)
# Long CALL: MAX(0,(underlying price at expiration-strike) * # contracts * contract size)

# Problem statement:
'''
Define the best strategy that, given the current options holding, maximizes the payoff.
The paramater should be:
- purchase prices of the options (necessary to compute the exit strategy) - only for purchased options
- current prices of all possible options
- maximizes the SUM of the expected payoff: 
    # Open new positions:
    SUM(
        MIN(0, - (underlying strike at expiration - current strike) * # contracts * contract size), --> Short PUT
        MAX(0,(underlying strike at expiration - current strike) * # contracts * contract size), --> Long PUT
        MIN(0, - (current strike - underlying strike at expiration) * # contracts * contract size), --> Short CALL
        MAX(0,(current strike - underlying strike at expiration) * # contracts * contract size)  --> Long CALL
    )
    # Closing old positions:
    SUM(
        (current_prices) * # contracts * contract size --> Long PUT/CALL,
        (current_prices) * # contracts * contract size --> Short PUT/CALL, in this case # contracts would be negative
    )
- constraints:
    looping over all strikes:
        # no more than 4 PUT options in the same strike --> # PUT contracts on same strike <= 4
        # no more than 4 CALL options in the same strike --> # CALL contracts on same strike <= 4
    # no more than 8 put options in the strategy --> SUM(# contracts) <= 8
'''

from ortools.linear_solver import pywraplp
import pandas as pd
import numpy as np
from itertools import product
import json

def create_data_model():
    df = pd.read_csv('data/maximize_payoff_input.csv', sep = ';')

    data = {}

    data['prices_coeff'] = df['Price_CALL'].values.tolist()
    data['prices_coeff'].extend(df['Price_PUT'].values.tolist())
    data['strike'] = df['Strike'].values.tolist()

    # data['bounds'] = [8, 8, -1/3]
    data['bounds'] = np.repeat(4, len(data['strike'])*2).tolist()
    data['constraints_coeff'] = np.repeat(1, len(data['strike'])*2).tolist()
    data['num_constraints'] = len(data['strike'])*2
    data['num_vars'] = len(data['strike'])*2

    call_options = df['Price_CALL'].values.tolist()
    put_options = df['Price_PUT'].values.tolist()
    strikes = df['Strike'].values.tolist()

    return data, call_options, put_options, strikes

data, call_options, put_options, strikes = create_data_model()

perms = product(data['strike'], repeat=2)

all_perms = {}
for perm in perms:
    # First strike perm[0] is purchased strike
    # Second strike perm[1] is expiration strike
    long_call = max(0, perm[1] - perm[0])
    short_call = min(0, perm[0] - perm[1])
    long_put = max(0, perm[0] - perm[1])
    short_put = min(0, perm[1] - perm[0])
    all_perms[perm] = [long_call, short_call, long_put, short_put]

len(all_perms.keys())

len(data['strike'])**2


# Create the mip solver with the SCIP backend.
solver = pywraplp.Solver.CreateSolver('SCIP')

options = {}
for s, c, p in zip(strikes, call_options, put_options):
    for s_exp in strikes:
        options[(s, s_exp, c, p)] = [
            solver.IntVar(0, 4, 'call_buy_s%is_exp%ic%ip%i' % (s, s_exp, c, p)), 
            solver.IntVar(0, 4, 'call_sell_s%is_exp%ic%ip%i' % (s, s_exp, c, p)),
            solver.IntVar(0, 4, 'put_buy_s%is_exp%ic%ip%i' % (s, s_exp, c, p)),
            solver.IntVar(0, 4, 'put_sell_s%is_exp%ic%ip%i' % (s, s_exp, c, p))
            ]

print('Number of variables =', solver.NumVariables()) # 31*31*4

# Max 4 options per purchase strike
same_strike_list = []
for k, v in options.items():
    solver.Add(sum(v) <= 4)


# Max 4 options for each purchase strike (e.g. for strike 3000, max 4 purchased call options)
# 31*4=124 constraints
for strike in data['strike']:
    long_call = [v[0] for k, v in options.items() if k[0] == strike]
    short_call = [v[1] for k, v in options.items() if k[0] == strike]
    long_put = [v[2] for k, v in options.items() if k[0] == strike]
    short_put = [v[3] for k, v in options.items() if k[0] == strike]
                
    solver.Add(sum(long_call) <= 4)
    solver.Add(sum(short_call) <= 4)
    solver.Add(sum(long_put) <= 4)
    solver.Add(sum(short_put) <= 4)
    solver.Add(sum(long_call) + sum(short_call) <= 4)
    solver.Add(sum(long_put) + sum(short_put) <= 4)

# Max 4 options for each expiration strike (e.g. for strike 3000, max 4 purchased call options)
# 31*4=124 constraints
for strike in data['strike']:
    long_call = [v[0] for k, v in options.items() if k[1] == strike]
    short_call = [v[1] for k, v in options.items() if k[1] == strike]
    long_put = [v[2] for k, v in options.items() if k[1] == strike]
    short_put = [v[3] for k, v in options.items() if k[1] == strike]
            
    solver.Add(sum(long_call) <= 4)
    solver.Add(sum(short_call) <= 4)
    solver.Add(sum(long_put) <= 4)
    solver.Add(sum(short_put) <= 4)

# Max 16 total options
list = []
for k, v in options.items():
    list.extend(v)

solver.Add(sum(list) <= 16)

# Max 8 call options
list_call = []
for k, v in options.items():
    list_call.extend(v[:2])
solver.Add(sum(list_call) <= 8)

# Max 8 put options
list_put = []
for k, v in options.items():
    list_put.extend(v[2:])
solver.Add(sum(list_put) <= 8)

# At most 2 short options for each long option
list_short = []
for k, v in options.items():
    list_short.extend((v[1], v[3]))

list_long = []
for k, v in options.items():
    list_long.extend((v[0], v[2]))

sumLongOpt = solver.NumVar(0, 100, 'n_long_options')
sumShortOpt = solver.NumVar(0, 100, 'n_short_options')
solver.Add(sumLongOpt == sum(list_long))
solver.Add(sumShortOpt == sum(list_short))
solver.Add(2*sumLongOpt - sumShortOpt >= 0)

print('Number of constraints =', solver.NumConstraints())

objective = solver.Objective()

# Coefficients are:
# - premium collected from short positions
# - premium paid for long positions
# - looping over all pair of (strike of option, expiration strike) --> underlying strike at expiration - current strike
# The variables to maximize are still the number of options to buy/sell and those will be equal among all pair of (strike of option, expiration strike)
# so different variables but same constraints for each of them

for (k, v), (k_options, v_options) in zip(all_perms.items(), options.items()):
    print("Sum of coefficients for {} is {}".format(v_options[1], v[1] + k_options[2]))
    '''    
    print("Permutations for couple {}".format(k))
    print("Purchase strike is {}".format(k[0]))
    print("Expiration strike is {}".format(k[1]))
    print("Value for call longs is {}".format(v[0]))
    print("Value for call shorts is {}".format(v[1]))
    print("Value for put longs is {}".format(v[2]))
    print("Value for put shorts is {}".format(v[3]))
    print("Variable for long call is {}".format(v_options[0]))
    '''
    
    # all combination for call longs
    objective.SetCoefficient(v_options[0], v[0] - k_options[2])
    # all combination for call shorts
    objective.SetCoefficient(v_options[1], v[1] + k_options[2])
    # all combination for put longs
    objective.SetCoefficient(v_options[2], v[2] - k_options[3])
    # all combination for put shorts
    objective.SetCoefficient(v_options[3], v[3] + k_options[3])

obj_expr = [
    (v[0] - k_options[2]) * v_options[0] + 
    (v[1] + k_options[2]) * v_options[1] + 
    (v[2] - k_options[3]) * v_options[2] + 
    (v[3] + k_options[3]) * v_options[3] 
    for (k, v), (k_options, v_options) in zip(all_perms.items(), options.items())
    ]

solver.Maximize(solver.Sum(obj_expr))

for var in solver.variables():
    print(var)
    print(objective.GetCoefficient(var))

objective.SetMaximization()
status = solver.Solve()

results = {}

if status == pywraplp.Solver.OPTIMAL:
    print('Objective value =', solver.Objective().Value())
    for k, v in options.items():
        results[''.join(str(k))] = [v[0].solution_value(), v[1].solution_value(), v[2].solution_value(), v[3].solution_value()]
        if sum([v[0].solution_value(), v[1].solution_value(), v[2].solution_value(), v[3].solution_value()]) > 0:
            print('Strike {}: {} long call, {} short call, {} long put, {} short put'.format(k, v[0].solution_value(), v[1].solution_value(), v[2].solution_value(), v[3].solution_value()))
    print('Problem solved in %f milliseconds' % solver.wall_time())
    print('Problem solved in %d iterations' % solver.iterations())
    print('Problem solved in %d branch-and-bound nodes' % solver.nodes())
else:
    print('The problem does not have an optimal solution.')

json.dump(results, open('data/results_maximize_payoff.json', 'w'))