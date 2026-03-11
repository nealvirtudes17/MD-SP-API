from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, DateTime, Float, Boolean, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class AllOrder(Base):
    __tablename__ = "All_Orders"

    # Surrogate Primary Key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Core Identifiers
    amazon_order_id: Mapped[Optional[str]] = mapped_column("amazon-order-id", String(50), index=True)
    merchant_order_id: Mapped[Optional[str]] = mapped_column("merchant-order-id", String(50), index=True)
    original_order_id: Mapped[Optional[str]] = mapped_column("original-order-id", String(50))
    purchase_order_number: Mapped[Optional[str]] = mapped_column("purchase-order-number", String(100))
    
    # Dates
    purchase_date: Mapped[Optional[datetime]] = mapped_column("purchaseDate", DateTime, index=True)
    last_updated_date: Mapped[Optional[datetime]] = mapped_column("last-updated-date", DateTime)
    
    # Order Status & Channels
    order_status: Mapped[Optional[str]] = mapped_column("order-status", String(50))
    fulfillment_channel: Mapped[Optional[str]] = mapped_column("fulfillment-channel", String(50))
    sales_channel: Mapped[Optional[str]] = mapped_column("sales-channel", String(50))
    order_channel: Mapped[Optional[str]] = mapped_column("order-channel", String(50))
    url: Mapped[Optional[str]] = mapped_column("url", Text)
    
    # Product Information
    product_name: Mapped[Optional[str]] = mapped_column("product-name", Text)
    sku: Mapped[Optional[str]] = mapped_column("sku", String(100), index=True)
    asin: Mapped[Optional[str]] = mapped_column("asin", String(50), index=True)
    item_status: Mapped[Optional[str]] = mapped_column("item-status", String(50))
    number_of_items: Mapped[Optional[int]] = mapped_column("number-of-items", Integer)
    quantity: Mapped[Optional[int]] = mapped_column("quantity", Integer)
    
    # Pricing & Taxes
    currency: Mapped[Optional[str]] = mapped_column("currency", String(10))
    item_price: Mapped[Optional[float]] = mapped_column("item-price", Float)
    item_tax: Mapped[Optional[float]] = mapped_column("item-tax", Float)
    shipping_price: Mapped[Optional[float]] = mapped_column("shipping-price", Float)
    shipping_tax: Mapped[Optional[float]] = mapped_column("shipping-tax", Float)
    gift_wrap_price: Mapped[Optional[float]] = mapped_column("gift-wrap-price", Float)
    gift_wrap_tax: Mapped[Optional[float]] = mapped_column("gift-wrap-tax", Float)
    item_promotion_discount: Mapped[Optional[float]] = mapped_column("item-promotion-discount", Float)
    ship_promotion_discount: Mapped[Optional[float]] = mapped_column("ship-promotion-discount", Float)
    
    # VAT Exclusive Pricing
    vat_exclusive_item_price: Mapped[Optional[float]] = mapped_column("vat-exclusive-item-price", Float)
    vat_exclusive_shipping_price: Mapped[Optional[float]] = mapped_column("vat-exclusive-shipping-price", Float)
    vat_exclusive_giftwrap_price: Mapped[Optional[float]] = mapped_column("vat-exclusive-giftwrap-price", Float)
    price_designation: Mapped[Optional[str]] = mapped_column("price-designation", String(50))
    
    # Shipping & Address
    ship_service_level: Mapped[Optional[str]] = mapped_column("ship-service-level", String(100))
    address_type: Mapped[Optional[str]] = mapped_column("address-type", String(50))
    ship_city: Mapped[Optional[str]] = mapped_column("ship-city", String(100))
    ship_state: Mapped[Optional[str]] = mapped_column("ship-state", String(100))
    ship_postal_code: Mapped[Optional[str]] = mapped_column("ship-postal-code", String(50))
    ship_country: Mapped[Optional[str]] = mapped_column("ship-country", String(10))
    
    # Promotions & Payments
    promotion_ids: Mapped[Optional[str]] = mapped_column("promotion-ids", Text)
    payment_method_details: Mapped[Optional[str]] = mapped_column("payment-method-details", String(100))
    item_extensions_data: Mapped[Optional[str]] = mapped_column("item-extensions-data", Text)
    
    # Flags & B2B Data
    is_business_order: Mapped[Optional[str]] = mapped_column("is-business-order", String(10))  # Often "true"/"false" strings in Amazon reports
    is_sold_by_ab: Mapped[Optional[str]] = mapped_column("is-sold-by-ab", String(10))
    is_amazon_invoiced: Mapped[Optional[str]] = mapped_column("is-amazon-invoiced", String(10))
    is_replacement_order: Mapped[Optional[str]] = mapped_column("is-replacement-order", String(10))
    is_exchange_order: Mapped[Optional[str]] = mapped_column("is-exchange-order", String(10))
    is_heavy_or_bulky: Mapped[Optional[str]] = mapped_column("is-heavy-or-bulky", String(10))
    is_buyer_requested_cancellation: Mapped[Optional[str]] = mapped_column("is-buyer-requested-cancellation", String(10))
    is_pickup_point_order: Mapped[Optional[str]] = mapped_column("is-pickup-point-order", String(10))
    fulfilled_by: Mapped[Optional[str]] = mapped_column("fulfilled-by", String(50))
    
    # Tax & Invoice Details
    buyer_company_name: Mapped[Optional[str]] = mapped_column("buyer-company-name", String(255))
    buyer_cst_number: Mapped[Optional[str]] = mapped_column("buyer-cst-number", String(100))
    buyer_vat_number: Mapped[Optional[str]] = mapped_column("buyer-vat-number", String(100))
    buyer_tax_registration_id: Mapped[Optional[str]] = mapped_column("buyer-tax-registration-id", String(100))
    buyer_tax_registration_country: Mapped[Optional[str]] = mapped_column("buyer-tax-registration-country", String(10))
    buyer_tax_registration_type: Mapped[Optional[str]] = mapped_column("buyer-tax-registration-type", String(50))
    buyer_citizen_name: Mapped[Optional[str]] = mapped_column("buyer-citizen-name", String(255))
    buyer_citizen_id: Mapped[Optional[str]] = mapped_column("buyer-citizen-id", String(100))
    order_invoice_type: Mapped[Optional[str]] = mapped_column("order-invoice-type", String(50))
    invoice_business_legal_name: Mapped[Optional[str]] = mapped_column("invoice-business-legal-name", String(255))
    invoice_business_address: Mapped[Optional[str]] = mapped_column("invoice-business-address", Text)
    invoice_business_tax_id: Mapped[Optional[str]] = mapped_column("invoice-business-tax-id", String(100))
    invoice_business_tax_office: Mapped[Optional[str]] = mapped_column("invoice-business-tax-office", String(100))
    
    # Customization & Licensing
    customized_url: Mapped[Optional[str]] = mapped_column("customized-url", Text)
    customized_page: Mapped[Optional[str]] = mapped_column("customized-page", Text)
    licensee_name: Mapped[Optional[str]] = mapped_column("licensee-name", String(255))
    license_number: Mapped[Optional[str]] = mapped_column("license-number", String(100))
    license_state: Mapped[Optional[str]] = mapped_column("license-state", String(100))
    license_expiration_date: Mapped[Optional[str]] = mapped_column("license-expiration-date", String(50))

