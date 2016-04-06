<?php

namespace OLAP;


class DataType extends Type {

    public function getSetData() {

        return $this->getOption('set_data');
    }

    public function getPushData() {

        return $this->getOption('push_data');
    }

    public function getAggregate() {

        return $this->getOption('aggregate');
    }

    public function getAggregateLinear() {

        return $this->getOption('aggregate_linear');
    }

    public function getPushMethod() {

        return $this->getOption('push_method');
    }

}