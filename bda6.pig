employees = LOAD 'employees.txt' USING PigStorage(',')
AS (id:int, name:chararray, dept:chararray, salary:int);

departments = LOAD 'departments.txt' USING PigStorage(',')
AS (dept_code:chararray, dept_name:chararray);

sorted_emp = ORDER employees BY salary DESC;
DUMP sorted_emp;

grouped_emp = GROUP employees BY dept;
DUMP grouped_emp;

joined_data = JOIN employees BY dept, departments BY dept_code;
joined_project = FOREACH joined_data GENERATE employees::id, employees::name, departments::dept_name, employees::salary;
DUMP joined_project;

projected_emp = FOREACH employees GENERATE name, salary;
DUMP projected_emp;

filtered_emp = FILTER employees BY salary > 55000;
DUMP filtered_emp;

group_count = GROUP employees BY dept;
dept_count = FOREACH group_count GENERATE group, COUNT(employees);
DUMP dept_count;

group_avg = GROUP employees BY dept;
dept_avg = FOREACH group_avg GENERATE group, AVG(employees.salary);
DUMP dept_avg;

high_sorted = ORDER employees BY salary DESC;
highest_paid = LIMIT high_sorted 1;
DUMP highest_paid;

dept_only = FOREACH employees GENERATE dept;
unique_dept = DISTINCT dept_only;
DUMP unique_dept;

top_sorted = ORDER employees BY salary DESC;
top_three = LIMIT top_sorted 3;
DUMP top_three;

low_sorted = ORDER employees BY salary ASC;
lowest_paid = LIMIT low_sorted 1;
DUMP lowest_paid;

above_60000 = FILTER employees BY salary > 60000;
all_group = GROUP above_60000 ALL;
count_above_60000 = FOREACH all_group GENERATE COUNT(above_60000);
DUMP count_above_60000;

grouped_top = GROUP employees BY dept;
top2_each_dept = FOREACH grouped_top {
    sorted_inside = ORDER employees BY salary DESC;
    top2_inside = LIMIT sorted_inside 2;
    GENERATE FLATTEN(top2_inside);
};
DUMP top2_each_dept;
