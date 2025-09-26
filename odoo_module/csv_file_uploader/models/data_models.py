from odoo import fields, models


class GlassReportRecord(models.Model):
    _name = "glass.report.record"
    _description = "Glass Report Record"
    _rec_name = "order_ref"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    order_date = fields.Char(string="Order Date")
    list_date = fields.Char(string="List Date")
    sealed_unit_id = fields.Char(string="Sealed Unit ID")
    ot = fields.Char(string="OT")
    window_type = fields.Char(string="Window Type")
    line1 = fields.Char(string="Line 1")
    line2 = fields.Char(string="Line 2")
    line3 = fields.Char(string="Line 3")
    grills = fields.Char(string="Grills")
    spacer = fields.Char(string="Spacer")
    dealer = fields.Char(string="Dealer")
    glass_comment = fields.Char(string="Glass Comment")
    tag = fields.Char(string="Tag")
    zones = fields.Char(string="Zones")
    u_value = fields.Char(string="U Value")
    solar_heat_gain = fields.Char(string="Solar Heat Gain")
    visual_trasmittance = fields.Char(string="Visual Transmittance")
    energy_rating = fields.Char(string="Energy Rating")
    glass_type = fields.Char(string="Glass Type")
    order_ref = fields.Char(string="Order")
    width = fields.Char(string="Width")
    height = fields.Char(string="Height")
    qty = fields.Char(string="Quantity")
    description = fields.Char(string="Description")
    note1 = fields.Char(string="Note 1")
    note2 = fields.Char(string="Note 2")
    rack_id = fields.Char(string="Rack ID")
    complete = fields.Char(string="Complete")
    shipping = fields.Char(string="Shipping")


class FramesCuttingRecord(models.Model):
    _name = "frames.cutting.record"
    _description = "Frames Cutting Record"
    _rec_name = "col_a"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    col_a = fields.Char(string="Column A")
    col_b = fields.Char(string="Column B")
    col_c = fields.Char(string="Column C")
    col_d = fields.Char(string="Column D")
    col_e = fields.Char(string="Column E")
    col_f = fields.Char(string="Column F")
    col_g = fields.Char(string="Column G")
    col_h = fields.Char(string="Column H")
    col_i = fields.Char(string="Column I")
    col_j = fields.Char(string="Column J")
    col_k = fields.Char(string="Column K")
    col_l = fields.Char(string="Column L")
    col_m = fields.Char(string="Column M")
    col_n = fields.Char(string="Column N")
    col_o = fields.Char(string="Column O")
    col_p = fields.Char(string="Column P")
    col_q = fields.Char(string="Column Q")
    col_r = fields.Char(string="Column R")
    col_s = fields.Char(string="Column S")
    col_t = fields.Char(string="Column T")
    col_u = fields.Char(string="Column U")
    col_v = fields.Char(string="Column V")
    col_w = fields.Char(string="Column W")
    col_x = fields.Char(string="Column X")
    col_y = fields.Char(string="Column Y")
    col_z = fields.Char(string="Column Z")

class CasingCuttingRecord(models.Model):
    _name = "casing.cutting.record"
    _description = "Casing Cutting Record"
    _rec_name = "order_ref"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    hw = fields.Char(string="H_W")
    bin_code = fields.Char(string="Bin")
    order_line = fields.Char(string="Order Line")
    material = fields.Char(string="Material")
    label = fields.Char(string="Label")
    order_ref = fields.Char(string="Order")
    window_name = fields.Char(string="Window")
    windows_size = fields.Char(string="Windows Size")
    rossette = fields.Char(string="Rossette")
    casing_line = fields.Char(string="Casing Line")
    company = fields.Char(string="Company")
    po = fields.Char(string="PO")
    order_date = fields.Char(string="Date")
    order_time = fields.Char(string="Time")
    user_name = fields.Char(string="User")


class InvoiceDateRecord(models.Model):
    _name = "invoice.date.record"
    _description = "Invoice Date Record"
    _rec_name = "order_number"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    order_number = fields.Char(string="Order Number")
    company = fields.Char(string="Company")
    invoice_date = fields.Char(string="Invoice Date")
    invoice_number = fields.Char(string="Invoice Number")
    live_or_test = fields.Char(string="Live Or Test")
    original_order = fields.Char(string="Original Order")


class QuotationToOrderRecord(models.Model):
    _name = "quotation.to.order.record"
    _description = "Quotation To Order Record"
    _rec_name = "quotation_number"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    quotation_number = fields.Char(string="Quotation Number")
    to_order_number = fields.Char(string="To Order Number")
    windows_qty = fields.Char(string="Windows Qty")
    line_qty = fields.Char(string="Line Qty")
    opening_qty = fields.Char(string="Opening Qty")
    user_name = fields.Char(string="User Name")
    order_date = fields.Char(string="Order Date")
    system = fields.Char(string="System")
    output_date = fields.Char(string="Output Date")
    dealer_name = fields.Char(string="Dealer Name")


class WindowEntryRecord(models.Model):
    _name = "window.entry.record"
    _description = "Window Entry Record"
    _rec_name = "order_number"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    order_number = fields.Char(string="Order Number")
    quotation_number = fields.Char(string="Quotation Number")
    windows_qty = fields.Char(string="Windows Qty")
    line_qty = fields.Char(string="Line Qty")
    opening_qty = fields.Char(string="Opening Qty")
    user_name = fields.Char(string="User Name")
    order_date = fields.Char(string="Order Date")
    system = fields.Char(string="System")
    output_date = fields.Char(string="Output Date")
    dealer_name = fields.Char(string="Dealer Name")


class WorkOrderRecord(models.Model):
    _name = "work.order.record"
    _description = "Work Order Record"
    _rec_name = "order_number"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    order_number = fields.Char(string="Order Number")
    po = fields.Char(string="PO")
    tag = fields.Char(string="Tag")
    dealer = fields.Char(string="Dealer")
    order_date = fields.Char(string="Order Date")
    due_date = fields.Char(string="Due Date")
    window_description = fields.Text(string="Window Description")
    description = fields.Text(string="Description")
    options = fields.Text(string="Options")
    qty = fields.Char(string="Quantity")
    line_reference = fields.Char(string="Line Reference")
    note = fields.Text(string="Note")


class WorkOrderAlternateRecord(models.Model):
    _name = "work.order.alternate.record"
    _description = "Work Order Alternate Record"
    _rec_name = "order_number"

    upload_id = fields.Many2one("csv.upload", string="CSV Upload", ondelete="cascade")
    order_number = fields.Char(string="Order Number")
    po = fields.Char(string="PO")
    tag = fields.Char(string="Tag")
    dealer = fields.Char(string="Dealer")
    order_date = fields.Char(string="Order Date")
    due_date = fields.Char(string="Due Date")
    window_description = fields.Text(string="Window Description")
    description = fields.Text(string="Description")
    qty = fields.Char(string="Quantity")
    line_reference = fields.Char(string="Line Reference")
    note = fields.Text(string="Note")

