# -*- coding: utf-8 -*-
"""
Created on 2017年1月14日

@author: who8736
"""
from flask_wtf import Form, FlaskForm
from wtforms import StringField, SelectMultipleField, SelectField, SubmitField
from wtforms.validators import DataRequired


class StockListForm(Form):
    stockList = StringField('stockList', validators=[DataRequired()])


class Select2MultipleField(SelectMultipleField):

    # noinspection PyArgumentList
    def __init__(self, label=None, validators=None,
                 # coerce=text_type,
                 choices=None, **kwargs):
        super().__init__(label, validators, coerce, choices, kwargs)
        self.data = ""

    def pre_validate(self, form):
        # Prevent "not a valid choice" error
        pass

    def process_formdata(self, valuelist):
        if valuelist:
            self.data = "|".join(valuelist)


class HoldForm(FlaskForm):
    selected = Select2MultipleField(u'持有股票', [],
                                    choices=[],
                                    # description=u"多选。无限选项。",
                                    render_kw={"multiple": "multiple",
                                               "data-tags": "1"})
    submit = SubmitField('保存')
