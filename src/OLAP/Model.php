<?php

namespace OLAP;


abstract class Model {

    protected $options = [];

    abstract public function getName();

    protected function getOption($key, $default = null) {

        return array_key_exists($key, $this->options) ? $this->options[$key] : $default;
    }
}