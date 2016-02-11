<?php

namespace OLAP;


abstract class Model {

    /**
     * @var array
     */
    protected $options = [];
    /**
     * @var string
     */
    protected $name;

    /**
     * @return string
     */
    public function getName() {

        return $this->makeName($this->name);
    }

    /**
     * @param string $key
     * @param mixed $default
     * @return mixed
     */
    protected function getOption($key, $default = null) {

        return array_key_exists($key, $this->options) ? $this->options[$key] : $default;
    }

    /**
     * @param $name
     * @return string
     */
    protected function makeName($name) {

        return $name;
    }
}