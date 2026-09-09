from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Set by the bundle as the "idp.source_path" pipeline configuration value, so
# that dev reads from idp_test_dev and prod reads from idp_test_prod. The
# fallback matches the dev target and only applies to ad-hoc runs.
SOURCE_PATH = spark.conf.get(
    "idp.source_path",
    "/Volumes/idp_test_dev/idp_test_project/raw_documents/incoming"
)


SUPPORTED_EXTENSIONS = [
    "pdf",
    "png",
    "jpg",
    "jpeg",
    "tif",
    "tiff",
    "doc",
    "docx",
    "ppt",
    "pptx",
]


DOCUMENT_EXTRACTION_SCHEMA = {
    "document_type": {
        "type": "string",
        "description": (
            "Classify the document as invoice, purchase_order, "
            "receipt, or other."
        ),
    },
    "document_number": {
        "type": "string",
        "description": (
            "The primary identifier of the document, such as an "
            "invoice number, purchase order number, or receipt number."
        ),
    },
    "invoice_number": {
        "type": "string",
        "description": "The invoice number, if present.",
    },
    "purchase_order_number": {
        "type": "string",
        "description": "The purchase order number, if present.",
    },
    "receipt_number": {
        "type": "string",
        "description": "The receipt number, if present.",
    },
    "document_date": {
        "type": "string",
        "description": "The document date formatted as YYYY-MM-DD.",
    },
    "due_date": {
        "type": "string",
        "description": "The payment due date formatted as YYYY-MM-DD.",
    },
    "requested_ship_date": {
        "type": "string",
        "description": (
            "The requested shipment date formatted as YYYY-MM-DD."
        ),
    },
    "currency": {
        "type": "string",
        "description": (
            "The three-letter ISO currency code, such as USD."
        ),
    },
    "seller_name": {
        "type": "string",
        "description": (
            "The vendor, seller, merchant, or organization issuing "
            "the document."
        ),
    },
    "buyer_name": {
        "type": "string",
        "description": (
            "The buyer, customer, or organization receiving "
            "the document."
        ),
    },
    "bill_to_address": {
        "type": "string",
        "description": "The complete bill-to address, if present.",
    },
    "ship_to_address": {
        "type": "string",
        "description": "The complete ship-to address, if present.",
    },
    "payment_method": {
        "type": "string",
        "description": (
            "The payment method. Do not return a complete payment "
            "card number."
        ),
    },
    "payment_terms": {
        "type": "string",
        "description": "The stated payment terms.",
    },
    "subtotal": {
        "type": "number",
        "description": (
            "The subtotal before tax, freight, and other charges."
        ),
    },
    "tax_amount": {
        "type": "number",
        "description": "The total tax amount.",
    },
    "shipping_amount": {
        "type": "number",
        "description": "The shipping or freight charge.",
    },
    "discount_amount": {
        "type": "number",
        "description": "The total discount amount.",
    },
    "total_amount": {
        "type": "number",
        "description": "The final total amount of the document.",
    },
    "notes": {
        "type": "string",
        "description": (
            "Business notes, remittance instructions, or other "
            "important document notes."
        ),
    },
    "line_items": {
        "type": "array",
        "description": (
            "All product, service, expense, or purchase line items."
        ),
        "items": {
            "type": "object",
            "properties": {
                "line_number": {
                    "type": "integer",
                    "description": (
                        "The displayed source line number, if present."
                    ),
                },
                "description": {
                    "type": "string",
                    "description": (
                        "The product, service, or expense description."
                    ),
                },
                "quantity": {
                    "type": "number",
                    "description": "The purchased quantity.",
                },
                "unit_price": {
                    "type": "number",
                    "description": "The price per unit.",
                },
                "amount": {
                    "type": "number",
                    "description": "The extended line amount.",
                },
            },
        }
    }
}

@dp.table(
    name="bronze_documents",
    comment=(
        "Raw business documents ingested incrementally from a "
        "Unity Catalog volume."
    ),
    table_properties={
        "quality": "bronze",
    },
)
def bronze_documents():
    source = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(SOURCE_PATH)
    )

    return (
        source
        .withColumn(
            "file_extension",
            F.lower(
                F.regexp_extract(
                    F.col("path"),
                    r"\.([^.]+)$",
                    1,
                )
            ),
        )
        .filter(
            F.col("file_extension").isin(SUPPORTED_EXTENSIONS)
        )
        .select(
            F.sha2(
                F.col("content"),
                256,
            ).alias("document_id"),
            F.col("path").alias("source_file_path"),
            F.regexp_extract(
                F.col("path"),
                r"([^/]+)$",
                1,
            ).alias("source_file_name"),
            F.col("file_extension"),
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias(
                "source_modified_at"
            ),
            F.col("content").alias("document_content"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

@dp.table(
    name="silver_parsed_documents",
    comment=(
        "Business documents parsed into structured Databricks "
        "document representations."
    ),
    table_properties={
        "quality": "silver",
    },
)
def silver_parsed_documents():
    bronze = spark.readStream.table("bronze_documents")

    return (
        bronze
        .withColumn(
            "parsed_document",
            F.ai_parse_document(
                F.col("document_content"),
                options={
                    "version": "2.0",
                    "descriptionElementTypes": "",
                },
            ),
        )
        .select(
            F.col("document_id"),
            F.col("source_file_path"),
            F.col("source_file_name"),
            F.col("file_extension"),
            F.col("file_size_bytes"),
            F.col("source_modified_at"),
            F.col("ingested_at"),
            F.col("parsed_document"),
            F.current_timestamp().alias("parsed_at"),
        )
    )

@dp.table(
    name="silver_extracted_documents",
    comment=(
        "Structured business fields extracted from parsed documents."
    ),
    table_properties={
        "quality": "silver",
    },
)
def silver_extracted_documents():
    parsed = spark.readStream.table(
        "silver_parsed_documents"
    )

    return (
        parsed
        .withColumn(
            "extracted_document",
            F.ai_extract(
                F.col("parsed_document"),
                schema=DOCUMENT_EXTRACTION_SCHEMA,
                options={
                    "version": "2.1",
                },
            ),
        )
        .withColumn(
            "extracted_at",
            F.current_timestamp(),
        )
    )

@dp.table(
    name="silver_documents",
    comment=(
        "Normalized and validated document-level business fields."
    ),
    table_properties={
        "quality": "silver",
    },
)
def silver_documents():
    extracted = spark.readStream.table(
        "silver_extracted_documents"
    )

    normalized = (
        extracted
        .select(
            F.col("document_id"),
            F.col("source_file_path"),
            F.col("source_file_name"),
            F.col("file_extension"),
            F.col("file_size_bytes"),
            F.col("source_modified_at"),
            F.col("ingested_at"),
            F.col("parsed_at"),
            F.col("extracted_at"),
            F.col("parsed_document"),
            F.col("extracted_document"),
            F.expr("extracted_document:response:document_type:value").cast("string").alias("document_type"),
            F.expr("extracted_document:response:document_number:value").cast("string").alias("document_number"),
            F.expr("extracted_document:response:invoice_number:value").cast("string").alias("invoice_number"),
            F.expr("extracted_document:response:purchase_order_number:value").cast("string").alias("purchase_order_number"),
            F.expr("extracted_document:response:receipt_number:value").cast("string").alias("receipt_number"),
            F.expr("extracted_document:response:document_date:value").cast("string").alias("document_date_text"),
            F.expr("extracted_document:response:due_date:value").cast("string").alias("due_date_text"),
            F.expr("extracted_document:response:requested_ship_date:value").cast("string").alias("requested_ship_date_text"),
            F.expr("extracted_document:response:currency:value").cast("string").alias("currency"),
            F.expr("extracted_document:response:seller_name:value").cast("string").alias("seller_name"),
            F.expr("extracted_document:response:buyer_name:value").cast("string").alias("buyer_name"),
            F.expr("extracted_document:response:bill_to_address:value").cast("string").alias("bill_to_address"),
            F.expr("extracted_document:response:ship_to_address:value").cast("string").alias("ship_to_address"),
            F.expr("extracted_document:response:payment_method:value").cast("string").alias("payment_method"),
            F.expr("extracted_document:response:payment_terms:value").cast("string").alias("payment_terms"),
            F.expr("extracted_document:response:notes:value").cast("string").alias("notes"),
            F.expr("extracted_document:response:subtotal:value").cast("double").alias("subtotal_raw"),
            F.expr("extracted_document:response:tax_amount:value").cast("double").alias("tax_amount_raw"),
            F.expr("extracted_document:response:shipping_amount:value").cast("double").alias("shipping_amount_raw"),
            F.expr("extracted_document:response:discount_amount:value").cast("double").alias("discount_amount_raw"),
            F.expr("extracted_document:response:total_amount:value").cast("double").alias("total_amount_raw"),
            F.expr("extracted_document:response:line_items").cast("array<variant>").alias("line_items"),
        )
    )

    typed = (
        normalized
        .withColumn(
            "document_type",
            F.lower(F.trim(F.col("document_type"))),
        )
        .withColumn(
            "document_number",
            F.trim(F.col("document_number")),
        )
        .withColumn(
            "invoice_number",
            F.trim(F.col("invoice_number")),
        )
        .withColumn(
            "purchase_order_number",
            F.trim(F.col("purchase_order_number")),
        )
        .withColumn(
            "receipt_number",
            F.trim(F.col("receipt_number")),
        )
        .withColumn(
            "document_date",
            F.to_date(F.col("document_date_text")),
        )
        .withColumn(
            "due_date",
            F.to_date(F.col("due_date_text")),
        )
        .withColumn(
            "requested_ship_date",
            F.to_date(
                F.col("requested_ship_date_text")
            ),
        )
        .withColumn(
            "currency",
            F.upper(F.trim(F.col("currency"))),
        )
        .withColumn(
            "seller_name",
            F.trim(F.col("seller_name")),
        )
        .withColumn(
            "buyer_name",
            F.trim(F.col("buyer_name")),
        )
        .withColumn(
            "subtotal",
            F.col("subtotal_raw").cast(
                T.DecimalType(18, 2)
            ),
        )
        .withColumn(
            "tax_amount",
            F.col("tax_amount_raw").cast(
                T.DecimalType(18, 2)
            ),
        )
        .withColumn(
            "shipping_amount",
            F.col("shipping_amount_raw").cast(
                T.DecimalType(18, 2)
            ),
        )
        .withColumn(
            # Documents state discounts either as a positive reduction
            # ("Discount 100.00") or as a negative adjustment ("-100.00"), and
            # ai_extract reports whichever the document shows. Normalize to a
            # positive magnitude so that the calculated_total formula below,
            # which subtracts the discount, cannot double-negate it.
            "discount_amount",
            F.abs(
                F.col("discount_amount_raw").cast(
                    T.DecimalType(18, 2)
                )
            ),
        )
        .withColumn(
            "total_amount",
            F.col("total_amount_raw").cast(
                T.DecimalType(18, 2)
            ),
        )
    )

    validated = (
        typed
        .withColumn(
            "business_document_number",
            F.coalesce(
                F.col("document_number"),
                F.col("invoice_number"),
                F.col("purchase_order_number"),
                F.col("receipt_number"),
            ),
        )
        .withColumn(
            "calculated_total",
            (
                F.coalesce(
                    F.col("subtotal"),
                    F.lit(0),
                )
                + F.coalesce(
                    F.col("tax_amount"),
                    F.lit(0),
                )
                + F.coalesce(
                    F.col("shipping_amount"),
                    F.lit(0),
                )
                - F.coalesce(
                    F.col("discount_amount"),
                    F.lit(0),
                )
            ).cast(T.DecimalType(18, 2)),
        )
        .withColumn(
            "total_variance",
            F.abs(
                F.col("total_amount")
                - F.col("calculated_total")
            ).cast(T.DecimalType(18, 2)),
        )
        .withColumn(
            "validation_status",
            F.when(
                F.col("document_type").isNull(),
                F.lit("MISSING_DOCUMENT_TYPE"),
            )
            .when(
                ~F.col("document_type").isin(
                    "invoice",
                    "purchase_order",
                    "receipt",
                ),
                F.lit("UNSUPPORTED_DOCUMENT_TYPE"),
            )
            .when(
                F.col(
                    "business_document_number"
                ).isNull(),
                F.lit("MISSING_DOCUMENT_NUMBER"),
            )
            .when(
                F.col("total_amount").isNull(),
                F.lit("MISSING_TOTAL_AMOUNT"),
            )
            .when(
                F.col("total_amount") < F.lit(0),
                F.lit("INVALID_TOTAL_AMOUNT"),
            )
            .when(
                F.col("total_variance") > F.lit(0.02),
                F.lit("TOTAL_MISMATCH"),
            )
            .otherwise(F.lit("VALID")),
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
    )

    return validated.select(
        F.col("document_id"),
        F.col("source_file_path"),
        F.col("source_file_name"),
        F.col("file_extension"),
        F.col("file_size_bytes"),
        F.col("source_modified_at"),
        F.col("ingested_at"),
        F.col("parsed_at"),
        F.col("extracted_at"),
        F.col("processed_at"),
        F.col("document_type"),
        F.col("business_document_number"),
        F.col("document_number"),
        F.col("invoice_number"),
        F.col("purchase_order_number"),
        F.col("receipt_number"),
        F.col("document_date"),
        F.col("due_date"),
        F.col("requested_ship_date"),
        F.col("currency"),
        F.col("seller_name"),
        F.col("buyer_name"),
        F.col("bill_to_address"),
        F.col("ship_to_address"),
        F.col("payment_method"),
        F.col("payment_terms"),
        F.col("notes"),
        F.col("subtotal"),
        F.col("tax_amount"),
        F.col("shipping_amount"),
        F.col("discount_amount"),
        F.col("total_amount"),
        F.col("calculated_total"),
        F.col("total_variance"),
        F.col("validation_status"),
        F.col("line_items"),
        F.col("parsed_document"),
        F.col("extracted_document")
    )

@dp.table(
    name="silver_document_lines",
    comment=(
        "Normalized line items extracted from business documents."
    ),
    table_properties={
        "quality": "silver",
    },
)
def silver_document_lines():
    documents = spark.readStream.table("silver_documents")

    exploded = (
        documents
        .select(
            F.col("document_id"),
            F.col("document_type"),
            F.col("business_document_number"),
            F.col("currency"),
            F.posexplode_outer(
                F.col("line_items")
            ).alias(
                "line_position",
                "line_item",
            ),
        )
        .filter(F.col("line_item").isNotNull())
    )

    normalized = (
        exploded
        .select(
            F.col("document_id"),
            F.col("document_type"),
            F.col("business_document_number"),
            F.col("currency"),
            (
                F.col("line_position") + F.lit(1)
            ).alias("derived_line_number"),
            F.expr("line_item:line_number:value").cast("int").alias("source_line_number"),
            F.expr("line_item:description:value").cast("string").alias("description"),
            F.expr("line_item:quantity:value").cast("double").alias("quantity_raw"),
            F.expr("line_item:unit_price:value").cast("double").alias("unit_price_raw"),
            F.expr("line_item:amount:value").cast("double").alias("line_amount_raw"),
        )
    )

    typed = (
        normalized
        .withColumn(
            "line_number",
            F.coalesce(
                F.col("source_line_number"),
                F.col("derived_line_number"),
            ),
        )
        .withColumn(
            "description",
            F.trim(F.col("description")),
        )
        .withColumn(
            "quantity",
            F.col("quantity_raw").cast(
                T.DecimalType(18, 4)
            ),
        )
        .withColumn(
            "unit_price",
            F.col("unit_price_raw").cast(
                T.DecimalType(18, 4)
            ),
        )
        .withColumn(
            "line_amount",
            F.col("line_amount_raw").cast(
                T.DecimalType(18, 2)
            ),
        )
        .withColumn(
            "calculated_line_amount",
            (
                F.col("quantity") * F.col("unit_price")
            ).cast(T.DecimalType(18, 2)),
        )
        .withColumn(
            "line_amount_variance",
            F.when(
                F.col("quantity").isNotNull()
                & F.col("unit_price").isNotNull()
                & F.col("line_amount").isNotNull(),
                F.abs(
                    F.col("line_amount")
                    - F.col("calculated_line_amount")
                ).cast(T.DecimalType(18, 2)),
            ),
        )
        .withColumn(
            "line_validation_status",
            F.when(
                F.col("description").isNull(),
                F.lit("MISSING_DESCRIPTION"),
            )
            .when(
                F.col("line_amount").isNull(),
                F.lit("MISSING_LINE_AMOUNT"),
            )
            .when(
                F.col("line_amount_variance")
                > F.lit(0.02),
                F.lit("LINE_AMOUNT_MISMATCH"),
            )
            .otherwise(F.lit("VALID")),
        )
    )

    return typed.select(
        F.col("document_id"),
        F.col("document_type"),
        F.col("business_document_number"),
        F.col("line_number"),
        F.col("description"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("line_amount"),
        F.col("calculated_line_amount"),
        F.col("line_amount_variance"),
        F.col("line_validation_status"),
        F.col("currency"),
    )

@dp.table(
    name="gold_documents",
    comment=(
        "Analytics-ready validated business document headers."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_documents():
    documents = spark.read.table("silver_documents")

    return (
        documents
        .filter(
            F.col("validation_status") == F.lit("VALID")
        )
    )

@dp.table(
    name="gold_document_lines",
    comment=(
        "Analytics-ready validated document line items."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_document_lines():
    lines = spark.read.table("silver_document_lines")

    valid_documents = (
        spark.read.table("silver_documents")
        .filter(
            F.col("validation_status") == F.lit("VALID")
        )
        .select(F.col("document_id"))
    )

    return (
        lines
        .join(
            valid_documents,
            on="document_id",
            how="inner",
        )
        .select(
            F.col("document_id"),
            F.col("document_type"),
            F.col("business_document_number"),
            F.col("line_number"),
            F.col("description"),
            F.col("quantity"),
            F.col("unit_price"),
            F.col("line_amount"),
            F.col("currency"),
            F.col("line_validation_status"),
        )
    )

@dp.table(
    name="gold_invoices",
    comment="Validated invoice headers.",
    table_properties={
        "quality": "gold",
    },
)
def gold_invoices():
    documents = spark.read.table("gold_documents")

    return (
        documents
        .filter(
            F.col("document_type") == F.lit("invoice")
        )
        .select(
            F.col("document_id"),
            F.col("invoice_number"),
            F.col("purchase_order_number"),
            F.col("document_date").alias("invoice_date"),
            F.col("due_date"),
            F.col("seller_name").alias("vendor_name"),
            F.col("buyer_name").alias("customer_name"),
            F.col("currency"),
            F.col("subtotal"),
            F.col("tax_amount"),
            F.col("shipping_amount"),
            F.col("discount_amount"),
            F.col("total_amount"),
            F.col("payment_terms"),
            F.col("source_file_name"),
            F.col("processed_at"),
        )
    )

@dp.table(
    name="gold_purchase_orders",
    comment="Validated purchase order headers.",
    table_properties={
        "quality": "gold",
    },
)
def gold_purchase_orders():
    documents = spark.read.table("gold_documents")

    return (
        documents
        .filter(
            F.col("document_type")
            == F.lit("purchase_order")
        )
        .select(
            F.col("document_id"),
            F.col("business_document_number").alias(
                "purchase_order_number"
            ),
            F.col("document_date").alias(
                "purchase_order_date"
            ),
            F.col("requested_ship_date"),
            F.col("buyer_name"),
            F.col("seller_name").alias("vendor_name"),
            F.col("currency"),
            F.col("subtotal"),
            F.col("tax_amount"),
            F.col("shipping_amount"),
            F.col("discount_amount"),
            F.col("total_amount"),
            F.col("source_file_name"),
            F.col("processed_at"),
        )
    )

@dp.table(
    name="gold_receipts",
    comment="Validated receipt headers.",
    table_properties={
        "quality": "gold",
    },
)
def gold_receipts():
    documents = spark.read.table("gold_documents")

    return (
        documents
        .filter(
            F.col("document_type") == F.lit("receipt")
        )
        .select(
            F.col("document_id"),
            F.col("receipt_number"),
            F.col("document_date").alias(
                "transaction_date"
            ),
            F.col("seller_name").alias("merchant_name"),
            F.col("currency"),
            F.col("subtotal"),
            F.col("tax_amount"),
            F.col("total_amount"),
            F.col("payment_method"),
            F.col("source_file_name"),
            F.col("processed_at"),
        )
    )

@dp.table(
    name="gold_vendor_spend",
    comment=(
        "Monthly vendor spend aggregated from invoices and receipts."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_vendor_spend():
    documents = (
        spark.read.table("gold_documents")
        .filter(
            F.col("document_type").isin(
                "invoice",
                "receipt",
            )
        )
    )

    return (
        documents
        .groupBy(
            F.date_trunc(
                "month",
                F.col("document_date"),
            ).alias("spend_month"),
            F.col("seller_name").alias("vendor_name"),
            F.col("currency"),
            F.col("document_type"),
        )
        .agg(
            F.countDistinct(
                F.col("document_id")
            ).alias("document_count"),
            F.sum(
                F.col("subtotal")
            ).alias("subtotal_amount"),
            F.sum(
                F.col("tax_amount")
            ).alias("tax_amount"),
            F.sum(
                F.col("shipping_amount")
            ).alias("shipping_amount"),
            F.sum(
                F.col("total_amount")
            ).alias("total_spend"),
        )
    )

@dp.table(
    name="gold_invoice_po_reconciliation",
    comment=(
        "Document-level reconciliation between invoices and "
        "purchase orders."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_invoice_po_reconciliation():
    invoices = (
        spark.read.table("gold_invoices")
        .alias("invoice")
    )

    purchase_orders = (
        spark.read.table("gold_purchase_orders")
        .alias("purchase_order")
    )

    join_condition = (
        F.upper(
            F.trim(
                F.col("invoice.purchase_order_number")
            )
        )
        == F.upper(
            F.trim(
                F.col(
                    "purchase_order.purchase_order_number"
                )
            )
        )
    )

    return (
        invoices
        .join(
            purchase_orders,
            on=join_condition,
            how="left",
        )
        .select(
            F.col("invoice.document_id").alias(
                "invoice_document_id"
            ),
            F.col("invoice.invoice_number"),
            F.col(
                "invoice.purchase_order_number"
            ).alias("invoice_po_number"),
            F.col("invoice.vendor_name").alias(
                "invoice_vendor_name"
            ),
            F.col("invoice.currency").alias(
                "invoice_currency"
            ),
            F.col("invoice.total_amount").alias(
                "invoice_total"
            ),
            F.col("purchase_order.document_id").alias(
                "purchase_order_document_id"
            ),
            F.col(
                "purchase_order.purchase_order_number"
            ).alias("matched_po_number"),
            F.col("purchase_order.vendor_name").alias(
                "purchase_order_vendor_name"
            ),
            F.col("purchase_order.currency").alias(
                "purchase_order_currency"
            ),
            F.col("purchase_order.total_amount").alias(
                "purchase_order_total"
            ),
            F.when(
                F.col(
                    "invoice.purchase_order_number"
                ).isNull(),
                F.lit("INVOICE_HAS_NO_PO_NUMBER"),
            )
            .when(
                F.col(
                    "purchase_order.document_id"
                ).isNull(),
                F.lit("PO_NOT_FOUND"),
            )
            .when(
                F.upper(
                    F.trim(
                        F.col("invoice.vendor_name")
                    )
                )
                != F.upper(
                    F.trim(
                        F.col(
                            "purchase_order.vendor_name"
                        )
                    )
                ),
                F.lit("VENDOR_MISMATCH"),
            )
            .when(
                F.col("invoice.currency")
                != F.col("purchase_order.currency"),
                F.lit("CURRENCY_MISMATCH"),
            )
            .when(
                F.col("invoice.total_amount")
                > F.col(
                    "purchase_order.total_amount"
                ),
                F.lit("INVOICE_EXCEEDS_PO"),
            )
            .otherwise(F.lit("MATCHED"))
            .alias("match_status"),
        )
    )
