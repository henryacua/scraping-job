"""Tests para backend.app.models (SQLModel)."""
import pytest
from backend.app.models import Business, BusinessStatus


class TestBusinessStatus:
    def test_status_values(self):
        assert BusinessStatus.PENDING.value == "PENDING"
        assert BusinessStatus.PROCESSING.value == "PROCESSING"
        assert BusinessStatus.LEAD_QUALIFIED.value == "LEAD_QUALIFIED"
        assert BusinessStatus.HAS_WEBSITE.value == "HAS_WEBSITE"
        assert BusinessStatus.FILTERED_OUT.value == "FILTERED_OUT"
        assert BusinessStatus.ERROR.value == "ERROR"

    def test_status_from_string(self):
        assert BusinessStatus("PENDING") == BusinessStatus.PENDING
        assert BusinessStatus("LEAD_QUALIFIED") == BusinessStatus.LEAD_QUALIFIED

    def test_status_invalid_raises(self):
        with pytest.raises(ValueError):
            BusinessStatus("INVALID")


class TestBusiness:
    def test_create_minimal(self):
        biz = Business(name="Test Corp")
        assert biz.name == "Test Corp"
        assert biz.phone is None
        assert biz.status == BusinessStatus.PENDING.value

    def test_create_full(self):
        biz = Business(
            name="Dental Clinic",
            phone="+57 300 1234567",
            address="Calle 10 #5-20",
            website="https://dental.com",
            search_query="Dentistas en Medellín",
            category="Dentista",
            rating="4.5",
        )
        assert biz.name == "Dental Clinic"
        assert biz.phone == "+57 300 1234567"
        assert biz.website == "https://dental.com"

    def test_model_dump(self):
        biz = Business(
            name="Test",
            phone="123",
            status=BusinessStatus.LEAD_QUALIFIED.value,
        )
        d = biz.model_dump()
        assert d["name"] == "Test"
        assert d["phone"] == "123"
        assert d["status"] == "LEAD_QUALIFIED"

    def test_model_validate_roundtrip(self):
        biz = Business(
            name="Roundtrip",
            phone="999",
            status=BusinessStatus.ERROR.value,
        )
        d = biz.model_dump()
        restored = Business.model_validate(d)
        assert restored.name == biz.name
        assert restored.phone == biz.phone
        assert restored.status == biz.status
