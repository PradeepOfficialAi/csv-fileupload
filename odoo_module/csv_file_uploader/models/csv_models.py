import logging
from odoo import api, fields, models, _
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)



class CsvUploadType(models.Model):
    _name = "csv.upload.type"
    _description = "CSV Upload Type"
    _order = "sequence, name"

    name = fields.Char(required=True)
    code = fields.Char(required=True, help="Unique code used by external clients to reference this profile.")
    description = fields.Text()
    sequence = fields.Integer(default=10)
    target_model = fields.Char(
        string="Target Model",
        help="Technical model name the data should be persisted in (e.g. glass.report)."
    )
    primary_key = fields.Char(
        help="Column name used to determine duplicates when importing."
    )
    secondary_key = fields.Char(
        help="Optional secondary column for duplicate detection (e.g. sealed_unit_id)."
    )
    notify_duplicates = fields.Boolean(default=True)
    notify_resends = fields.Boolean(default=False)
    active = fields.Boolean(default=True)
    field_schema = fields.Json(
        string="Field Schema",
        help="JSON structure describing expected CSV columns, their types and any validation rules."
    )
    note = fields.Text()

    _sql_constraints = [
        ("csv_upload_type_code_unique", "unique(code)", "The upload type code must be unique."),
    ]


class CsvUpload(models.Model):
    _name = "csv.upload"
    _description = "CSV Upload"
    _order = "create_date desc"

    name = fields.Char(required=True, default=lambda self: _("New CSV Upload"))
    type_id = fields.Many2one("csv.upload.type", required=True, ondelete="restrict")
    state = fields.Selection(
        [
            ("draft", "Draft"),
            ("processing", "Processing"),
            ("done", "Completed"),
            ("error", "Failed"),
        ],
        default="draft",
        required=True,
        tracking=True,
    )
    upload_user_id = fields.Many2one("res.users", default=lambda self: self.env.user, readonly=True)
    upload_datetime = fields.Datetime(default=fields.Datetime.now, readonly=True)
    original_filename = fields.Char()
    source_system = fields.Char(help="Optional source identifier provided by the desktop client.")
    file_data = fields.Binary(string="CSV File", required=True, attachment=True)
    row_count = fields.Integer()
    duplicate_count = fields.Integer()
    error_count = fields.Integer()
    log_message = fields.Text(readonly=True)
    line_ids = fields.One2many("csv.upload.line", "upload_id")

    def action_mark_processing(self):
        for record in self:
            if record.state not in ("draft", "error"):
                raise ValidationError(_("Only draft or failed uploads can be re-processed."))
            record.state = "processing"

    def action_mark_done(self):
        for record in self:
            record.state = "done"

    def action_mark_done_from_client(self):
        for record in self:
            with self.env.cr.savepoint():
                with self.env.registry.cursor() as new_cr:
                    new_env = self.env(cr=new_cr)
                    record_in_new_env = new_env['csv.upload'].browse(record.id)
                    try:
                        _logger.info(f"Marking record {record.id} as done with new cursor")
                        record_in_new_env.write({"state": "done"})
                    except Exception as e:
                        _logger.error(f"Error marking record {record.id} as done with new cursor: {e}", exc_info=True)
                        raise
        return True
    def action_mark_error(self, message):
        for record in self:
            record.write({
                "state": "error",
                "log_message": message,
            })

    def register_results(self, row_total=0, duplicates=0, errors=0):
        for record in self:
            with self.env.cr.savepoint():
                with self.env.registry.cursor() as new_cr:
                    new_env = self.env(cr=new_cr)
                    record_in_new_env = new_env['csv.upload'].browse(record.id)
                    try:
                        vals = {
                            "row_count": int(row_total or 0),
                            "duplicate_count": int(duplicates or 0),
                            "error_count": int(errors or 0),
                        }
                        _logger.info(f"Writing to csv.upload record {record.id} with new cursor: {vals}")
                        record_in_new_env.write(vals)
                    except Exception as e:
                        _logger.error(f"Error writing to csv.upload record {record.id} with new cursor: {e}", exc_info=True)
                        raise
        return True


class CsvUploadLine(models.Model):
    _name = "csv.upload.line"
    _description = "CSV Upload Line"
    _order = "row_index"

    upload_id = fields.Many2one("csv.upload", required=True, ondelete="cascade")
    row_index = fields.Integer(required=True)
    payload = fields.Json(required=True, help="Row data mapped into a structured JSON object.")
    status = fields.Selection(
        [
            ("new", "New"),
            ("duplicate", "Duplicate"),
            ("resent", "Resent"),
            ("error", "Error"),
        ],
        default="new",
        required=True,
    )
    message = fields.Char(help="Optional status message or validation feedback.")

    _sql_constraints = [
        (
            "csv_upload_line_unique",
            "unique(upload_id, row_index)",
            "Each row index must be unique per upload.",
        )
    ]


class CsvUploadLog(models.Model):
    _name = "csv.upload.log"
    _description = "CSV Upload Log"
    _order = "create_date desc"

    upload_id = fields.Many2one("csv.upload", required=True, ondelete="cascade")
    level = fields.Selection(
        [("info", "Info"), ("warning", "Warning"), ("error", "Error")],
        default="info",
    )
    message = fields.Text(required=True)
    metadata = fields.Json(help="Optional payload for structured logging (e.g. duplicates, headers).")


