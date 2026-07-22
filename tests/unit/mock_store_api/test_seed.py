from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
@patch("services.mock_store_api.seed.AsyncSessionLocal")
async def test_seed_skips_if_already_has_data(mock_session_maker):
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_maker.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 5  # 5 customers already exist
    mock_session.execute = AsyncMock(return_value=mock_result)

    from services.mock_store_api.seed import seed_data_if_empty

    await seed_data_if_empty()

    mock_session.add_all.assert_not_called()
    mock_session.commit.assert_not_called()


@pytest.mark.asyncio
@patch("services.mock_store_api.seed.AsyncSessionLocal")
async def test_seed_inserts_data_when_empty(mock_session_maker):
    mock_session = AsyncMock()
    mock_session.add_all = MagicMock()
    mock_session_maker.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_maker.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 0  # no customers yet
    mock_session.execute = AsyncMock(return_value=mock_result)

    from services.mock_store_api.seed import seed_data_if_empty

    await seed_data_if_empty()

    assert mock_session.add_all.call_count == 3  # customers, products, orders
    mock_session.commit.assert_called_once()
