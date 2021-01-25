def test_get_taxons_with_vds_prefixes_correctly():
    # TODO add logic here
    pass
    # mock_get_taxons.return_value = [
    #     Taxon.construct(
    #         slug='vds_slug|taxon_slug',
    #         display_name='Display Name',
    #         acronym='acr',
    #         company_slug='company_slug',
    #         data_source='vds_slug',
    #         taxon_group='Custom',
    #         validation_type='text',
    #         taxon_type='dimension',
    #         aggregation_type=None,
    #     )
    # ]
    #
    # taxons = TaxonomyService.get_taxons('company_id', 'company_slug', vds_slug='vds_slug', limit=0, offset=0)
    #
    # assert mock_get_taxons.mock_calls == [
    #     call(company_id='company_id', company_slug='company_slug', virtual_data_source='vds_slug', limit=0, offset=0)
    # ]
    # assert taxons == [
    #     FdqTaxon(
    #         slug='vds_slug|taxon_slug',
    #         display_name='Display Name',
    #         acronym='acr',
    #         data_source='vds_slug',
    #         taxon_type='dimension',
    #         group='Custom',
    #         data_type='text',
    #         field_type='dimension',
    #         validation_type='text',
    #     )
    # ]


def test_get_taxons_prefixes_correctly():
    # TODO add logic here
    pass
    # mock_get_taxons.return_value = [
    #     Taxon.construct(
    #         slug='company_slug__taxon_slug',
    #         display_name='Display Name',
    #         company_slug='company_slug',
    #         taxon_group='Custom',
    #         validation_type='text',
    #         taxon_type='dimension',
    #         aggregation_type=None,
    #         data_source=None,
    #     )
    # ]
    #
    # taxons = TaxonomyService.get_taxons('company_id', 'company_slug', vds_slug=None, limit=0, offset=0)
    #
    # assert mock_get_taxons.mock_calls == [
    #     call(company_id='company_id', company_slug='company_slug', virtual_data_source=None, limit=0, offset=0)
    # ]
    # assert taxons == [
    #     FdqTaxon(
    #         slug='taxon_slug',
    #         display_name='Display Name',
    #         taxon_type='dimension',
    #         group='Custom',
    #         data_type='text',
    #         field_type='dimension',
    #         validation_type='text',
    #     )
    # ]
