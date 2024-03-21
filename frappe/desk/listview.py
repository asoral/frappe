# Copyright (c) 2019, Frappe Technologies Pvt. Ltd. and Contributors
# MIT License. See license.txt
from __future__ import unicode_literals
from frappe.query_builder.functions import Count
from frappe.query_builder.terms import SubQuery
from frappe.query_builder.utils import DocType
import frappe
from typing import Dict, List

@frappe.whitelist()
def get_list_settings(doctype):
	try:
		return frappe.get_cached_doc("List View Settings", doctype)
	except frappe.DoesNotExistError:
		frappe.clear_messages()

@frappe.whitelist()
def set_list_settings(doctype, values):
	try:
		doc = frappe.get_doc("List View Settings", doctype)
	except frappe.DoesNotExistError:
		doc = frappe.new_doc("List View Settings")
		doc.name = doctype
		frappe.clear_messages()
	doc.update(frappe.parse_json(values))
	doc.save()


@frappe.whitelist()
def get_group_by_count(doctype: str, current_filters: str, field: str) -> List[Dict]:
	current_filters = frappe.parse_json(current_filters)
	# subquery_condition = ''

	# subquery = frappe.get_all(doctype, filters=current_filters, return_query = True)
	if field == 'assigned_to':
	# 	subquery_condition = ' and `tabToDo`.reference_name in ({subquery})'.format(subquery = subquery)
	# 	return frappe.db.sql("""select `tabToDo`.owner as name, count(*) as count
	# 		from
	# 			`tabToDo`, `tabUser`
	# 		where
	# 			`tabToDo`.status!='Cancelled' and
	# 			`tabToDo`.owner = `tabUser`.name and
	# 			`tabUser`.user_type = 'System User'
	# 			{subquery_condition}
	# 		group by
	# 			`tabToDo`.owner
	# 		order by
	# 			count desc
	# 		limit 50""".format(subquery_condition = subquery_condition), as_dict=True)
	# else:
	# 	return frappe.db.get_list(doctype,
	# 		filters=current_filters,
	# 		group_by='`tab{0}`.{1}'.format(doctype, field),
	# 		fields=['count(*) as count', '`{}` as name'.format(field)],
	# 		order_by='count desc',
	# 		limit=50,
	# 	)
		ToDo = DocType("ToDo")
		User = DocType("User")
		count = Count("*").as_("count")
		filtered_records = frappe.db.query.build_conditions(doctype, current_filters).select("name")

		return (
			frappe.qb.from_(ToDo)
			.from_(User)
			.select(ToDo.allocated_to.as_("name"), count)
			.where(
				(ToDo.status != "Cancelled")
				& (ToDo.allocated_to == User.name)
				& (User.user_type == "System User")
				& (ToDo.reference_name.isin(SubQuery(filtered_records)))
			)
			.groupby(ToDo.allocated_to)
			.orderby(count)
			.limit(50)
			.run(as_dict=True)
		)

	return frappe.get_list(
		doctype,
		filters=current_filters,
		group_by=f"`tab{doctype}`.{field}",
		fields=["count(*) as count", f"`{field}` as name"],
		order_by="count desc",
		limit=50,
	)

